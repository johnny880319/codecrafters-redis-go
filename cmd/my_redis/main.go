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

	db, err := database.NewDatabase(dbConfig)
	if err != nil {
		fmt.Println("Error initializing database: ", err.Error())
		os.Exit(1)
	}

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
	dbConfig := database.DBConfig{
		Port: "6379",
		Role: "master",
	}

	for i := 0; i+1 < len(args); i += 2 {
		switch args[i] {
		case "--port":
			dbConfig.Port = args[i+1]
		case "--replicaof":
			dbConfig.Role = "slave"
			parts := strings.Fields(args[i+1])
			if len(parts) != 2 {
				return database.DBConfig{}, fmt.Errorf("invalid value for --replicaof")
			}
			dbConfig.MasterAddr = net.JoinHostPort(parts[0], parts[1])
		case "--dir":
			dbConfig.Dir = args[i+1]
		case "--dbfilename":
			dbConfig.DBFilename = args[i+1]
		}
	}
	return dbConfig, nil
}
