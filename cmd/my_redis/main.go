package main

import (
	"context"
	"fmt"
	"net"
	"os"
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
		}(conn)
	}
}

func generateResponse(buf []byte) []byte {
	// PING
	if string(buf) == "*1\r\n$4\r\nPING\r\n" {
		return []byte("+PONG\r\n")
	}
	// ECHO
	if len(buf) > 0 && string(buf[:14]) == "*2\r\n$4\r\nECHO\r\n" {
		return buf[14:]
	}
	return []byte("-ERR unknown command\r\n")
}
