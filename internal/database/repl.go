package database

import (
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
func (db *Database) RunConnection(c net.Conn) {
	defer func() {
		err := c.Close()
		if err != nil {
			fmt.Println("Error closing connection: ", err.Error())
		}
	}()

	buf := make([]byte, 1024)
	for {
		n, err := c.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		fmt.Printf("Received command: %s", string(buf[:n]))
		response := db.generateResponse(buf[:n])
		fmt.Printf("Sending response: %s", string(response))
		_, err = c.Write(response)
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
	}
}

func (db *Database) generateResponse(buf []byte) []byte {
	args, err := parseCommand(buf)
	if err != nil {
		return []byte("-ERR invalid command format\r\n")
	}
	if len(args) == 0 {
		return []byte("-ERR empty command\r\n")
	}
	return db.executeCommand(args[0], args[1:])
}
