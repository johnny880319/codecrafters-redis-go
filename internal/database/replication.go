package database

import (
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

	if _, err := conn.Write(respArray([][]byte{bulkString("PING", true)})); err != nil {
		return err
	}
	return nil
}
