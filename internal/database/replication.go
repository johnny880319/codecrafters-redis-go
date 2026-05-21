package database

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
)

// RunReplication connects to the master and starts the replication process.
func (db *Database) RunReplication(masterAddr string) (err error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(context.Background(), "tcp", masterAddr)
	if err != nil {
		return err
	}
	defer func() {
		connErr := conn.Close()
		err = errors.Join(err, connErr)
	}()

	handshakes := [][]byte{
		respArray([][]byte{bulkString("PING", true)}),
		respArray([][]byte{bulkString("REPLCONF", true), bulkString("listening-port", true), bulkString(db.port, true)}),
		respArray([][]byte{bulkString("REPLCONF", true), bulkString("capa", true), bulkString("psync2", true)}),
		respArray([][]byte{bulkString("PSYNC", true), bulkString("?", true), bulkString("-1", true)}),
	}

	reader := bufio.NewReader(conn)
	for _, handshake := range handshakes {
		if _, err := conn.Write(handshake); err != nil {
			return err
		}
		if _, err := readResponse(reader); err != nil {
			return err
		}
	}
	err = readRDB(reader)
	if err != nil {
		return err
	}

	client := &client{db: db, conn: conn, watched: make(map[string]string)}

	for {
		command, _, err := readCommand(reader)
		if err != nil {
			return fmt.Errorf("error reading command: %w", err)
		}
		if len(command) == 0 {
			continue
		}

		_ = client.handleCommand(command)
	}
}

func readRDB(reader *bufio.Reader) error {
	line, err := readRespLine(reader)
	if err != nil {
		return err
	}
	if len(line) == 0 || line[0] != '$' {
		return fmt.Errorf("invalid RDB format")
	}

	rdbLen, err := strconv.Atoi(line[1:])
	if err != nil || rdbLen < 0 {
		return fmt.Errorf("invalid RDB length: %w", err)
	}

	buf := make([]byte, rdbLen)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return fmt.Errorf("error reading RDB data: %w", err)
	}

	return nil
}
