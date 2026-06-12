package database

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

func (c *client) cmdInfo(args []string) []byte {
	if len(args) != 1 {
		return simpleError(redisErr, "usage: INFO replication")
	}
	switch args[0] {
	case "replication":
		return bulkString(
			fmt.Sprintf("role:%v\n", c.db.config.Role)+
				fmt.Sprintf("master_replid:%v\n", c.db.masterReplid)+
				"master_repl_offset:0\n",
			true,
		)
	default:
		return simpleError(redisErr, "unsupported INFO section")
	}
}

func (c *client) cmdReplconf(args []string) []byte {
	if len(args) != 2 {
		return simpleError(redisErr, "usage: REPLCONF <option> <value>")
	}

	if args[0] == "listening-port" || args[0] == "capa" {
		return simpleString("OK")
	}

	if args[0] != "ACK" {
		return simpleError(redisErr, "invalid REPLCONF option")
	}

	replicaOffset, err := strconv.Atoi(args[1])
	if err != nil {
		return simpleError(redisErr, "invalid offset value from replica")
	}
	c.replicaOffset = replicaOffset
	return nil
}

func (c *client) cmdPsync(_ []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()
	c.db.replicas = append(c.db.replicas, c)

	response := simpleString(fmt.Sprintf("FULLRESYNC %v 0", c.db.masterReplid))

	emptyRDB, err := emptyRDBPayload()
	if err != nil {
		return simpleError(redisErr, "failed to generate empty RDB payload: "+err.Error())
	}

	response = append(response, []byte(fmt.Sprintf("$%d\r\n", len(emptyRDB)))...)
	response = append(response, emptyRDB...)
	return response
}

func emptyRDBPayload() ([]byte, error) {
	emptyRDBBase64 := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPo" +
		"FY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

	emptyRDB, err := base64.StdEncoding.DecodeString(emptyRDBBase64)
	if err != nil {
		return nil, fmt.Errorf("ERR failed to decode empty RDB data: %w", err)
	}
	return emptyRDB, nil
}

func (c *client) cmdWait(args []string) []byte {
	if len(args) != 2 {
		return simpleError(redisErr, "usage: WAIT <replica count> <timeout>")
	}
	count, err := strconv.Atoi(args[0])
	if err != nil || count < 0 {
		return simpleError(redisErr, "invalid count value")
	}
	timeoutMs, err := strconv.Atoi(args[1])
	if err != nil || timeoutMs < 0 {
		return simpleError(redisErr, "invalid timeout value")
	}

	c.db.rwMu.Lock()
	replicas := make([]*client, len(c.db.replicas))
	copy(replicas, c.db.replicas)
	c.db.rwMu.Unlock()

	for _, replica := range replicas {
		if _, err := replica.conn.Write(respArray([][]byte{
			bulkString("REPLCONF", true),
			bulkString("GETACK", true),
			bulkString("*", true),
		})); err != nil {
			return simpleError(redisErr, "error sending REPLCONF GETACK to replica: "+err.Error())
		}
	}

	timer := time.NewTimer(time.Duration(timeoutMs) * time.Millisecond)
	defer timer.Stop()

	for {
		syncCount := 0
		for _, replica := range replicas {
			if replica.replicaOffset >= c.offset {
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
