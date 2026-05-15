package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/database"
)

func main() {
	// read arg --port
	port, role := parseArgs(os.Args[1:])

	db := database.NewDatabase(role)

	lc := net.ListenConfig{}

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:"+port)
	if err != nil {
		fmt.Println("Failed to bind to port ", port, ": ", err.Error())
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
			err := db.RunConnection(c)
			if err != nil {
				fmt.Println("Error handling connection: ", err.Error())
			}
		}(conn)
	}
}

func parseArgs(args []string) (port string, role string) {
	port = "6379"
	role = "master"
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if i+1 < len(args) {
				port = args[i+1]
				i++
			}
		case "--replicaof":
			role = "slave"
			i += 2 // skip host and port
		}
	}
	return port, role
}
