package database

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

func (c *client) cmdInfo(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'INFO' command")
	}
	switch args[0] {
	case "replication":
		return bulkString(
			fmt.Sprintf("role:%v\n", c.db.role)+
				fmt.Sprintf("master_replid:%v\n", c.db.masterReplid)+
				"master_repl_offset:0\n",
			true,
		)
	default:
		return simpleError("unsupported INFO section")
	}
}

func (c *client) cmdReplconf(args []string) []byte {
	if len(args) != 2 {
		return simpleError("invalid response from replica")
	}

	if args[0] == "listening-port" || args[0] == "capa" {
		return simpleString("OK")
	}

	if args[0] != "ACK" {
		return simpleError("invalid REPLCONF option")
	}

	replicaOffset, err := strconv.Atoi(args[1])
	if err != nil {
		return simpleError("invalid offset value from replica")
	}
	c.replicaOffset = replicaOffset
	return nil
}

func (c *client) cmdPsync(_ []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.clients = append(c.db.clients, c)

	response := simpleString(fmt.Sprintf("FULLRESYNC %v 0", c.db.masterReplid))

	emptyRDBBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPo" +
		"FY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	emptyRDB, err := base64.StdEncoding.DecodeString(emptyRDBBase64)
	if err != nil {
		return simpleError("failed to decode empty RDB data")
	}

	response = append(response, []byte(fmt.Sprintf("$%d\r\n%s", len(emptyRDB), emptyRDB))...)
	return response
}

func (c *client) cmdWait(args []string) []byte {
	if len(args) != 2 {
		return simpleError("wrong number of arguments for 'WAIT' command")
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count < 0 {
		return simpleError("invalid count value")
	}
	timeoutMs, err := strconv.Atoi(args[1])
	if err != nil || timeoutMs < 0 {
		return simpleError("invalid timeout value")
	}

	for _, replicaClient := range c.db.clients {
		if _, err := replicaClient.conn.Write(respArray([][]byte{
			bulkString("REPLCONF", true),
			bulkString("GETACK", true),
			bulkString("*", true),
		})); err != nil {
			return simpleError("error sending REPLCONF GETACK to replica")
		}
	}

	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()

	for {
		syncCount := 0
		for _, replicaClient := range c.db.clients {
			if replicaClient.replicaOffset >= c.offset {
				syncCount++
			}
		}
		if syncCount >= count {
			return respInteger(syncCount)
		}

		select {
		case <-timer.C:
			return respInteger(syncCount)
		default:
			time.Sleep(10 * time.Millisecond) // Avoid busy waiting
		}
	}
}
