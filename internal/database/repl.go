package database

import (
	"bufio"
	"errors"
	"fmt"
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
	err := creatAppendOnlyFile(config)
	if err != nil {
		return nil, fmt.Errorf("error initializing database: %w", err)
	}
	data, err := readRDBFile(config)
	if err != nil {
		return nil, fmt.Errorf("error initializing database: %w", err)
	}
	return &Database{
		config:       config,
		masterReplid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		data:         data,
		waiters:      make(map[string][]chan string),
	}, nil
}

func creatAppendOnlyFile(config DBConfig) error {
	if config.Appendonly != "yes" {
		return nil
	}
	appendOnlyPath := filepath.Join(config.Dir, config.Appenddirname)
	if _, err := os.Stat(appendOnlyPath); os.IsNotExist(err) {
		err := os.Mkdir(appendOnlyPath, 0o750)
		if err != nil {
			return fmt.Errorf("error creating appendonly directory: %w", err)
		}
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
