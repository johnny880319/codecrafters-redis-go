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
	mu         sync.RWMutex
	data       map[string]dbEntry
	waiters    map[string][]chan string
	commandMap map[string]func(args []string) []byte
}

// NewDatabase initializes and returns a new Database instance.
func NewDatabase() *Database {
	return &Database{
		data:    make(map[string]dbEntry),
		waiters: make(map[string][]chan string),
	}
}

func (db *Database) getCommandMap() map[string]func(args []string) []byte {
	return map[string]func(args []string) []byte{
		"ping":   db.cmdPing,
		"echo":   db.cmdEcho,
		"set":    db.cmdSet,
		"get":    db.cmdGet,
		"rpush":  db.cmdRpush,
		"lpush":  db.cmdLpush,
		"lpop":   db.cmdLpop,
		"blpop":  db.cmdBLpop,
		"lrange": db.cmdLrange,
		"llen":   db.cmdLlen,
		"type":   db.cmdType,
		"xadd":   db.cmdXadd,
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

	db.commandMap = db.getCommandMap()

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
	cmd := strings.ToLower(args[0])
	if handler, ok := db.commandMap[cmd]; ok {
		return handler(args[1:])
	}
	// unknown command
	return []byte("-ERR unknown command\r\n")
}
