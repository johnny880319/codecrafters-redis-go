package database

import "fmt"

func (c *client) cmdInfo(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'INFO' command")
	}
	switch args[0] {
	case "replication":
		return bulkString(fmt.Sprintf("role:%s\n", c.db.role), true)
	default:
		return simpleError("unsupported INFO section")
	}
}
