package database

func (c *client) cmdInfo(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'INFO' command")
	}
	switch args[0] {
	case "replication":
		return bulkString("role:master\n", true)
	default:
		return simpleError("unsupported INFO section")
	}
}
