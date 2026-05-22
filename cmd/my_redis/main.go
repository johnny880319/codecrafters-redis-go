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
	dbConfig, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Println("Error parsing arguments: ", err.Error())
		os.Exit(1)
	}

	db := database.NewDatabase(dbConfig)

	lc := net.ListenConfig{}

	l, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:"+dbConfig.Port)
	if err != nil {
		fmt.Println("Failed to bind to port ", dbConfig.Port, ": ", err.Error())
		os.Exit(1)
	}
	defer func() {
		err := l.Close()
		if err != nil {
			fmt.Println("Error closing listener: ", err.Error())
		}
	}()

	if dbConfig.Role == "slave" {
		go func() {
			err := db.RunReplication(dbConfig.MasterAddr)
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

func parseArgs(args []string) (database.DBConfig, error) {
	port := "6379"
	role := "master"
	masterAddr := ""
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--port":
			if len(args) <= i+1 {
				return database.DBConfig{}, fmt.Errorf("missing value for --port")
			}
			port = args[i+1]
			i++
		case "--replicaof":
			if len(args) <= i+1 {
				return database.DBConfig{}, fmt.Errorf("missing value for --replicaof")
			}
			role = "slave"
			parts := strings.Fields(args[i+1])
			if len(parts) != 2 {
				return database.DBConfig{}, fmt.Errorf("invalid value for --replicaof")
			}
			masterAddr = net.JoinHostPort(parts[0], parts[1])
			i++
		}
	}
	return database.DBConfig{
		Role:       role,
		Port:       port,
		MasterAddr: masterAddr,
	}, nil
}
