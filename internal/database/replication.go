package database

import (
	"bufio"
	"context"
	"errors"
	"net"
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
	return nil
}
