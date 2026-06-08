package database

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DBConfig holds the configuration for the Database, including role, port, and master address.
type DBConfig struct {
	Role           string
	Port           string
	MasterAddr     string
	Dir            string
	DBFilename     string
	Appendonly     string
	Appenddirname  string
	Appendfilename string
	Appendfsync    string
}

// ValueType represents the type of value stored in the database (e.g., string, list).
type ValueType int

// constants for different value types.
const (
	StringType ValueType = iota
	ListType
	StreamType
)

type dbEntry struct {
	value     any
	vType     ValueType
	expiresAt time.Time
}

// Database represents an in-memory key-value store with command handling capabilities.
type Database struct {
	config       DBConfig
	masterReplid string
	mu           sync.RWMutex
	data         map[string]dbEntry
	waiters      map[string][]chan string
	replicas     []*client
	aofFile      *os.File
}

type client struct {
	db *Database

	conn          net.Conn
	offset        int
	replicaOffset int
	isMulti       bool
	// Tracks string snapshots for WATCH; version tracking would detect modify-and-restore cases.
	watched  map[string]string
	cmdQueue [][]string
}

// NewDatabase initializes and returns a new Database instance.
func NewDatabase(config DBConfig) (*Database, error) {
	db := &Database{
		config:       config,
		masterReplid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		waiters:      make(map[string][]chan string),
		data:         make(map[string]dbEntry),
	}
	if err := db.readRDBFile(config); err != nil {
		return nil, fmt.Errorf("error initializing database: %w", err)
	}
	if err := db.initializeAppendOnlyFile(config); err != nil {
		return nil, fmt.Errorf("error initializing database: %w", err)
	}
	return db, nil
}

//nolint:gocognit // Will refactor in the future.
func (db *Database) initializeAppendOnlyFile(config DBConfig) (err error) {
	if config.Appendonly != "yes" {
		return nil
	}
	appendOnlyDir := filepath.Join(config.Dir, config.Appenddirname)
	if _, err := os.Stat(appendOnlyDir); os.IsNotExist(err) {
		err = os.MkdirAll(appendOnlyDir, 0o750)
		if err != nil {
			return fmt.Errorf("error creating appendonly directory: %w", err)
		}
	}

	appendOnlyPath := filepath.Join(appendOnlyDir, config.Appendfilename+".1.incr.aof")
	manifestPath := filepath.Join(appendOnlyDir, config.Appendfilename+".manifest")
	// if exists, extract the appendonly file name.
	if _, err := os.Stat(manifestPath); err == nil {
		//nolint:gosec // This is redis behavior, we can assume the filename is safe
		content, err := os.ReadFile(manifestPath)
		if err != nil {
			return fmt.Errorf("error reading appendonly manifest file: %w", err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			// file <filename> seq <number> type <type>
			parts := strings.Fields(line)
			if len(parts) != 6 || parts[0] != "file" || parts[2] != "seq" || parts[4] != "type" {
				continue
			}
			if parts[5] != "i" {
				continue
			}
			appendOnlyPath = filepath.Join(appendOnlyDir, parts[1])
			break
		}
	}

	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		//nolint:gosec // This is redis behavior, we can assume the filename is safe
		file, err := os.Create(manifestPath)
		if err != nil {
			return fmt.Errorf("error creating appendonly manifest file: %w", err)
		}
		description := fmt.Sprintf("file %s seq 1 type i", config.Appendfilename+".1.incr.aof")
		_, err = file.WriteString(description)
		if err != nil {
			return fmt.Errorf("error writing to appendonly manifest file: %w", err)
		}
		err = file.Close()
		if err != nil {
			return fmt.Errorf("error closing appendonly manifest file: %w", err)
		}
	}

	if _, err := os.Stat(appendOnlyPath); os.IsNotExist(err) {
		//nolint:gosec // This is redis behavior, we can assume the filename is safe
		file, err := os.Create(appendOnlyPath)
		if err != nil {
			return fmt.Errorf("error creating appendonly file: %w", err)
		}
		err = file.Close()
		if err != nil {
			return fmt.Errorf("error closing appendonly file: %w", err)
		}
	}

	//nolint:gosec // This is redis behavior, we can assume the filename is safe
	aofFile, err := os.OpenFile(appendOnlyPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("error opening appendonly file: %w", err)
	}
	db.aofFile = aofFile

	//nolint:gosec // This is redis behavior, we can assume the filename is safe
	replayFile, err := os.Open(appendOnlyPath)
	if err != nil {
		return fmt.Errorf("error opening appendonly file for replay: %w", err)
	}
	defer func() {
		replayErr := replayFile.Close()
		err = errors.Join(err, replayErr)
	}()

	reader := bufio.NewReader(replayFile)
	virtualClient := &client{db: db, conn: nil, watched: make(map[string]string)}
	for {
		command, _, err := readCommand(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading appendonly file: %w", err)
		}

		if len(command) == 0 {
			continue
		}

		_ = virtualClient.handleCommand(command)
	}
	return nil
}

// RunConnection handles a single client connection, reading commands and writing responses.
func (db *Database) RunConnection(conn net.Conn) (err error) {
	defer func() {
		closeErr := conn.Close()
		err = errors.Join(err, closeErr)
	}()

	client := &client{db: db, conn: conn, watched: make(map[string]string)}

	reader := bufio.NewReader(conn)
	for {
		command, originalCommand, err := readCommand(reader)
		if err != nil {
			return fmt.Errorf("error reading command: %w", err)
		}
		if len(command) == 0 {
			continue
		}

		response := client.handleCommand(command)
		if len(response) == 0 {
			continue
		}
		err = client.appendFSync(command, originalCommand)
		if err != nil {
			return err
		}
		_, err = conn.Write(response)
		if err != nil {
			return fmt.Errorf("error writing response: %w", err)
		}
		if response[0] == '-' {
			continue
		}
		err = client.propagateToReplicas(command, originalCommand)
		if err != nil {
			return err
		}
	}
}

func (c *client) appendFSync(command []string, originalCommand []byte) error {
	if c.db.config.Appendonly != "yes" {
		return nil
	}
	if !isWriteCommand(command[0]) {
		return nil
	}
	if c.db.aofFile == nil {
		return fmt.Errorf("appendonly file is not initialized")
	}
	_, err := c.db.aofFile.Write(originalCommand)
	if err != nil {
		return fmt.Errorf("error writing to appendonly file: %w", err)
	}
	if c.db.config.Appendfsync == "always" {
		err = c.db.aofFile.Sync()
		if err != nil {
			return fmt.Errorf("error syncing appendonly file: %w", err)
		}
	}
	return nil
}

func (c *client) propagateToReplicas(command []string, originalCommand []byte) error {
	if !isWriteCommand(command[0]) {
		return nil
	}

	for _, replicaClient := range c.db.replicas {
		_, err := replicaClient.conn.Write(originalCommand)
		if err != nil {
			return fmt.Errorf("error propagating to replica: %w", err)
		}
	}
	c.offset += len(originalCommand)
	return nil
}

func isWriteCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "SET", "INCR", "RPUSH", "LPUSH", "LPOP", "XADD":
		return true
	default:
		return false
	}
}

func (c *client) handleCommand(command []string) []byte {
	cmd, args := command[0], command[1:]
	switch strings.ToUpper(cmd) {
	case "MULTI":
		return c.cmdMulti(args)
	case "EXEC":
		return c.cmdExec(args)
	case "DISCARD":
		return c.cmdDiscard(args)
	case "WATCH":
		return c.cmdWatch(args)
	case "UNWATCH":
		return c.cmdUnwatch(args)
	}

	if c.isMulti {
		c.cmdQueue = append(c.cmdQueue, command)
		return simpleString("QUEUED")
	}
	return c.executeCommand(command)
}

func (c *client) executeCommand(command []string) []byte {
	cmd, args := command[0], command[1:]
	switch strings.ToUpper(cmd) {
	case "PING":
		return c.cmdPing(args)
	case "ECHO":
		return c.cmdEcho(args)
	case "SET":
		return c.cmdSet(args)
	case "GET":
		return c.cmdGet(args)
	case "TYPE":
		return c.cmdType(args)
	case "INCR":
		return c.cmdIncr(args)
	case "CONFIG":
		return c.cmdConfig(args)
	case "KEYS":
		return c.cmdKeys(args)
	case "RPUSH":
		return c.cmdRpush(args)
	case "LPUSH":
		return c.cmdLpush(args)
	case "LPOP":
		return c.cmdLpop(args)
	case "BLPOP":
		return c.cmdBLpop(args)
	case "LRANGE":
		return c.cmdLrange(args)
	case "LLEN":
		return c.cmdLlen(args)
	case "XADD":
		return c.cmdXadd(args)
	case "XRANGE":
		return c.cmdXrange(args)
	case "XREAD":
		return c.cmdXread(args)
	case "INFO":
		return c.cmdInfo(args)
	case "REPLCONF":
		return c.cmdReplconf(args)
	case "PSYNC":
		return c.cmdPsync(args)
	case "WAIT":
		return c.cmdWait(args)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}
