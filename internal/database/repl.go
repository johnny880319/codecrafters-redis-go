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
		fmt.Println("Received", n, "bytes: ", string(buf[:n]))
		n, err = c.Write(db.generateResponse(buf[:n]))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
		fmt.Println("Sent", n, "bytes")
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

func parseCommand(buf []byte) ([]string, error) {
	if len(buf) == 0 || buf[0] != '*' {
		return nil, fmt.Errorf("invalid command format")
	}

	offset := 1
	// get number of arguments
	numArgs := 0
	for i := 1; buf[i] != '\r'; i++ {
		numArgs = numArgs*10 + int(buf[i]-'0')
		offset++
	}
	offset += 2 // skip \r\n

	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		if buf[offset] != '$' {
			return nil, fmt.Errorf("invalid argument format")
		}
		offset++

		argLen := 0
		for j := offset; buf[j] != '\r'; j++ {
			argLen = argLen*10 + int(buf[j]-'0')
			offset++
		}
		offset += 2 // skip \r\n

		args[i] = string(buf[offset : offset+argLen])
		offset += argLen + 2 // skip argument and \r\n
	}
	return args, nil
}
