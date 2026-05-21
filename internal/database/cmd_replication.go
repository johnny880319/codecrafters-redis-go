package database

import (
	"encoding/base64"
	"fmt"
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

func (c *client) cmdReplconf(_ []string) []byte {
	return simpleString("OK")
}

func (c *client) cmdPsync(_ []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.replicaConns = append(c.db.replicaConns, c.conn)

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
	return respInteger(0)
}
