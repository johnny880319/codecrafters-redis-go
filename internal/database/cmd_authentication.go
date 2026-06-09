package database

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func (c *client) cmdAcl(args []string) []byte {
	if len(args) < 1 {
		return simpleError("usage: ACL <subcommand> [arguments]")
	}

	switch strings.ToUpper(args[0]) {
	case "WHOAMI":
		return bulkString(c.currentUser, true)
	case "GETUSER":
		if len(args) != 2 {
			return simpleError("usage: ACL GETUSER <username>")
		}
		user := args[1]
		if _, exists := c.db.userProperties[user]; !exists {
			return simpleError("ERR no such user")
		}

		response := make([][]byte, 0)
		for key, values := range c.db.userProperties[user] {
			response = append(response, bulkString(key, true))
			subResponse := make([][]byte, 0)
			for _, value := range values {
				subResponse = append(subResponse, bulkString(value, true))
			}
			response = append(response, respArray(subResponse))
		}
		return respArray(response)
	case "SETUSER":
		if len(args) != 3 {
			return simpleError("usage: ACL SETUSER <username> ><password>")
		}
		if args[2][0] != '>' {
			return simpleError("ERR password must start with '>'")
		}

		user := args[1]
		password := args[2][1:] // Remove the '>' prefix
		passwordHash32 := sha256.Sum256([]byte(password))
		passwordHash := hex.EncodeToString(passwordHash32[:])
		c.db.userProperties[user]["flags"] = make([]string, 0)
		c.db.userProperties[user]["passwords"] = append(c.db.userProperties[user]["passwords"], passwordHash)
		return simpleString("OK")
	default:
		return simpleError("ERR unknown ACL subcommand")
	}
}
