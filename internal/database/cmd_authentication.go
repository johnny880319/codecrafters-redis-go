package database

import "strings"

func (c *client) cmdAcl(args []string) []byte {
	if len(args) != 1 {
		return simpleError("Usage: ACL <subcommand>")
	}

	switch strings.ToUpper(args[0]) {
	case "WHOAMI":
		return bulkString("default", true)
	default:
		return simpleError("ERR unknown ACL subcommand")
	}
}
