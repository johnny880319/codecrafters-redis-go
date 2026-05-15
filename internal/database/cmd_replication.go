package database

import "fmt"

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
	return simpleString(fmt.Sprintf("FULLRESYNC %v 0", c.db.masterReplid))
}
