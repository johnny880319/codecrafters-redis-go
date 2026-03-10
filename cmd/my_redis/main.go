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
				_, err := c.Read(buf)
				if err != nil {
					fmt.Println("Error reading from connection: ", err.Error())
					return
				}
				n, err := c.Write([]byte("+PONG\r\n"))
				if err != nil {
					fmt.Println("Error writing to connection: ", err.Error())
					return
				}
				fmt.Println("Sent", n, "bytes")
			}
		}(conn)
	}
}
