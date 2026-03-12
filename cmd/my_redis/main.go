package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type entry struct {
	value     string
	expiresAt time.Time
}

type database struct {
	data map[string]entry
}

func main() {
	db := &database{
		data: make(map[string]entry),
	}

	lc := net.ListenConfig{}

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379: ", err.Error())
		os.Exit(1)
	}
	defer func() {
		err := l.Close()
		if err != nil {
			fmt.Println("Error closing listener: ", err.Error())
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}

		go func(c net.Conn) {
			db.runConnection(c)
		}(conn)
	}
}

func (db *database) runConnection(c net.Conn) {
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
		fmt.Println("Received", n, "bytes: ", string(buf[:n]))
		n, err = c.Write(db.generateResponse(buf[:n]))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
		fmt.Println("Sent", n, "bytes")
	}
}

//nolint:gocognit // Will refactor command handling in future iterations, so keeping it simple for now
func (db *database) generateResponse(buf []byte) []byte {
	args, err := parseCommand(buf)
	if err != nil {
		return []byte("-ERR invalid command format\r\n")
	}
	if len(args) == 0 {
		return []byte("-ERR empty command\r\n")
	}
	// PING (transform to lowercase for simplicity)
	if strings.ToLower(args[0]) == "ping" && len(args) == 1 {
		return []byte("+PONG\r\n")
	}
	// ECHO
	if strings.ToLower(args[0]) == "echo" && len(args) == 2 {
		return []byte("$" + strconv.Itoa(len(args[1])) + "\r\n" + args[1] + "\r\n")
	}
	// SET and GET
	if strings.ToLower(args[0]) == "set" && len(args) == 3 {
		db.data[args[1]] = entry{value: args[2]}
		return []byte("+OK\r\n")
	}
	if strings.ToLower(args[0]) == "set" && len(args) == 5 && strings.ToLower(args[3]) == "px" {
		px, err := strconv.Atoi(args[4])
		if err != nil {
			return []byte("-ERR invalid PX value\r\n")
		}
		db.data[args[1]] = entry{
			value:     args[2],
			expiresAt: time.Now().Add(time.Duration(px) * time.Millisecond),
		}
		return []byte("+OK\r\n")
	}
	if strings.ToLower(args[0]) == "set" && len(args) == 5 && strings.ToLower(args[3]) == "ex" {
		ex, err := strconv.Atoi(args[4])
		if err != nil {
			return []byte("-ERR invalid EX value\r\n")
		}
		db.data[args[1]] = entry{
			value:     args[2],
			expiresAt: time.Now().Add(time.Duration(ex) * time.Second),
		}
		return []byte("+OK\r\n")
	}
	if strings.ToLower(args[0]) == "get" && len(args) == 2 {
		val, ok := db.data[args[1]]
		if !ok {
			return []byte("$-1\r\n") // nil response
		}
		// check if the key has expired
		if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
			delete(db.data, args[1])
			return []byte("$-1\r\n") // nil response
		}
		return []byte("$" + strconv.Itoa(len(val.value)) + "\r\n" + val.value + "\r\n")
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
