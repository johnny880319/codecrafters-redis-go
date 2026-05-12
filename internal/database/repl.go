package database

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

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
	mu      sync.RWMutex
	data    map[string]dbEntry
	waiters map[string][]chan string
}

type client struct {
	db *Database

	isMulti  bool
	cmdQueue [][]string
}

// NewDatabase initializes and returns a new Database instance.
func NewDatabase() *Database {
	return &Database{
		data:    make(map[string]dbEntry),
		waiters: make(map[string][]chan string),
	}
}

func (c *client) executeCommand(cmd string, args []string) []byte {
	switch strings.ToLower(cmd) {
	case "ping":
		return c.cmdPing(args)
	case "echo":
		return c.cmdEcho(args)
	case "set":
		return c.cmdSet(args)
	case "get":
		return c.cmdGet(args)
	case "type":
		return c.cmdType(args)
	case "rpush":
		return c.cmdRpush(args)
	case "lpush":
		return c.cmdLpush(args)
	case "lpop":
		return c.cmdLpop(args)
	case "blpop":
		return c.cmdBLpop(args)
	case "lrange":
		return c.cmdLrange(args)
	case "llen":
		return c.cmdLlen(args)
	case "xadd":
		return c.cmdXadd(args)
	case "xrange":
		return c.cmdXrange(args)
	case "xread":
		return c.cmdXread(args)
	case "incr":
		return c.cmdIncr(args)
	case "multi":
		return c.cmdMulti(args)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}

// RunConnection handles a single client connection, reading commands and writing responses.
func (db *Database) RunConnection(conn net.Conn) (err error) {
	defer func() {
		closeErr := conn.Close()
		err = errors.Join(err, closeErr)
	}()

	client := &client{db: db}

	reader := bufio.NewReader(conn)
	for {
		command, err := readCommand(reader)
		if err != nil {
			return fmt.Errorf("error reading command: %w", err)
		}
		if len(command) == 0 {
			continue
		}

		response := client.handleCommand(command)
		_, err = conn.Write(response)
		if err != nil {
			return fmt.Errorf("error writing response: %w", err)
		}
	}
}

func (c *client) handleCommand(command []string) []byte {
	if strings.ToLower(command[0]) == "exec" {
		return c.cmdExec(command[1:])
	}

	if c.isMulti {
		c.cmdQueue = append(c.cmdQueue, command)
		return simpleString("QUEUED")
	}
	return c.executeCommand(command[0], command[1:])
}
