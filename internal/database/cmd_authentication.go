package database

import "strings"

func (c *client) cmdAcl(args []string) []byte {
	if len(args) < 1 {
		return simpleError("ERR wrong number of arguments for 'ACL' command")
	}

	switch strings.ToUpper(args[0]) {
	case "WHOAMI":
		return bulkString("default", true)
	case "GETUSER":
		return respArray([][]byte{bulkString("flags", true), respArray([][]byte{})})
	default:
		return simpleError("ERR unknown ACL subcommand")
	}
}
