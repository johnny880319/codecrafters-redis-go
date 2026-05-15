package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/database"
)

func main() {
	// read arg --port
	port, role, masterAddr, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Println("Error parsing arguments: ", err.Error())
		os.Exit(1)
	}

	db := database.NewDatabase(role, port)

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

	if role == "slave" {
		go func() {
			err := db.RunReplication(masterAddr)
			if err != nil {
				fmt.Println("Error running replication: ", err.Error())
			}
		}()
	}

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

func parseArgs(args []string) (port string, role string, masterAddr string, err error) {
	port = "6379"
	role = "master"
	masterAddr = ""
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if len(args) <= i+1 {
				return "", "", "", fmt.Errorf("missing value for --port")
			}
			port = args[i+1]
			i++
		case "--replicaof":
			if len(args) <= i+1 {
				return "", "", "", fmt.Errorf("missing value for --replicaof")
			}
			role = "slave"
			parts := strings.Fields(args[i+1])
			if len(parts) != 2 {
				return "", "", "", fmt.Errorf("invalid value for --replicaof")
			}
			masterAddr = net.JoinHostPort(parts[0], parts[1])
			i++
		}
	}
	return port, role, masterAddr, nil
}
