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
	port := "6379"
	if len(os.Args) > 2 && os.Args[1] == "--port" {
		port = os.Args[2]
	}

	db := database.NewDatabase()

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
