package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
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
			runConnection(c)
		}(conn)
	}
}

func runConnection(c net.Conn) {
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
		n, err = c.Write(generateResponse(buf[:n]))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
			return
		}
		fmt.Println("Sent", n, "bytes")
	}
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

func generateResponse(buf []byte) []byte {
	args, err := parseCommand(buf)
	if err != nil {
		return []byte("-ERR invalid command format\r\n")
	}
	// PING (transform to lowercase for simplicity)
	if len(args) == 1 && strings.ToLower(args[0]) == "ping" {
		return []byte("+PONG\r\n")
	}
	// ECHO
	if len(args) == 2 && strings.ToLower(args[0]) == "echo" {
		return []byte("+" + args[1] + "\r\n")
	}
	// unknown command
	return []byte("-ERR unknown command\r\n")
}
