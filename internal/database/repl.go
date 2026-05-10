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

// NewDatabase initializes and returns a new Database instance.
func NewDatabase() *Database {
	return &Database{
		data:    make(map[string]dbEntry),
		waiters: make(map[string][]chan string),
	}
}

func (db *Database) executeCommand(cmd string, args []string) []byte {
	switch strings.ToLower(cmd) {
	case "ping":
		return db.cmdPing(args)
	case "echo":
		return db.cmdEcho(args)
	case "set":
		return db.cmdSet(args)
	case "get":
		return db.cmdGet(args)
	case "type":
		return db.cmdType(args)
	case "rpush":
		return db.cmdRpush(args)
	case "lpush":
		return db.cmdLpush(args)
	case "lpop":
		return db.cmdLpop(args)
	case "blpop":
		return db.cmdBLpop(args)
	case "lrange":
		return db.cmdLrange(args)
	case "llen":
		return db.cmdLlen(args)
	case "xadd":
		return db.cmdXadd(args)
	case "xrange":
		return db.cmdXrange(args)
	case "xread":
		return db.cmdXread(args)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}

// RunConnection handles a single client connection, reading commands and writing responses.
func (db *Database) RunConnection(c net.Conn) (err error) {
	defer func() {
		closeErr := c.Close()
		err = errors.Join(err, closeErr)
	}()

	reader := bufio.NewReader(c)
	for {
		command, err := readCommand(reader)
		if err != nil {
			return fmt.Errorf("error reading command: %w", err)
		}
		if len(command) == 0 {
			continue
		}

		response := db.executeCommand(command[0], command[1:])
		_, err = c.Write(response)
		if err != nil {
			return fmt.Errorf("error writing response: %w", err)
		}
	}
}
