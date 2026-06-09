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
			return simpleError("no such user")
		}

		responseFlags := make([][]byte, len(c.db.userProperties[user].flags))
		for i, flag := range c.db.userProperties[user].flags {
			responseFlags[i] = bulkString(flag, true)
		}
		responsePasswords := make([][]byte, len(c.db.userProperties[user].passwords))
		for i, password := range c.db.userProperties[user].passwords {
			responsePasswords[i] = bulkString(password, true)
		}

		response := [][]byte{
			bulkString("flags", true),
			respArray(responseFlags),
			bulkString("passwords", true),
			respArray(responsePasswords),
		}
		return respArray(response)
	case "SETUSER":
		if len(args) != 3 {
			return simpleError("usage: ACL SETUSER <username> ><password>")
		}
		if args[2][0] != '>' {
			return simpleError("password must start with '>'")
		}

		user := args[1]
		password := args[2][1:] // Remove the '>' prefix
		passwordHash32 := sha256.Sum256([]byte(password))
		passwordHash := hex.EncodeToString(passwordHash32[:])

		c.db.userProperties[user] = userProperties{
			flags:     []string{},
			passwords: append(c.db.userProperties[user].passwords, passwordHash),
		}
		return simpleString("OK")
	default:
		return simpleError("unknown ACL subcommand")
	}
}

func (c *client) cmdAuth(args []string) []byte {
	if len(args) != 2 {
		return simpleError("usage: AUTH <username> <password>")
	}

	username := args[0]
	password := args[1]
	passwordHash32 := sha256.Sum256([]byte(password))
	passwordHash := hex.EncodeToString(passwordHash32[:])

	if _, exists := c.db.userProperties[username]; !exists {
		return simpleError(wrongPass)
	}

	for _, property := range c.db.userProperties[username].flags {
		if property == "nopass" {
			c.hasAuthenticated = true
			return simpleString("OK")
		}
	}

	for _, storedHash := range c.db.userProperties[username].passwords {
		if storedHash == passwordHash {
			c.hasAuthenticated = true
			return simpleString("OK")
		}
	}

	return simpleError(wrongPass)
}
