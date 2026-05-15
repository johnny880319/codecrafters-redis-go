package database

import "fmt"

func (c *client) cmdInfo(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'INFO' command")
	}
	switch args[0] {
	case "replication":
		return bulkString(c.db.getRoleInfo(), true)
	default:
		return simpleError("unsupported INFO section")
	}
}

func (db *Database) getRoleInfo() string {
	return fmt.Sprintf("role:%v\n", db.role) +
		"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\n" +
		"master_repl_offset:0\n"
}
