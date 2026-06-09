package database

import "strings"

func (c *client) cmdAcl(args []string) []byte {
	if len(args) < 1 {
		return simpleError("ERR wrong number of arguments for 'ACL' command")
	}

	switch strings.ToUpper(args[0]) {
	case "WHOAMI":
		return bulkString(c.currentUser, true)
	case "GETUSER":
		response := make([][]byte, 0)
		for key, values := range c.db.userProperties[c.currentUser] {
			response = append(response, bulkString(key, true))
			subResponse := make([][]byte, 0)
			for _, value := range values {
				subResponse = append(subResponse, bulkString(value, true))
			}
			response = append(response, respArray(subResponse))
		}
		return respArray(response)
	default:
		return simpleError("ERR unknown ACL subcommand")
	}
}
