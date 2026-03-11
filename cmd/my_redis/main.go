package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment the code below to pass the first stage

	l, err := net.Listen("tcp", "127.0.0.1:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
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
	if len(buf) > 0 && string(buf[:20]) == "*2\r\n$4\r\nECHO\r\n" {
		return buf[20:]
	}
	return []byte("-ERR unknown command\r\n")
}
