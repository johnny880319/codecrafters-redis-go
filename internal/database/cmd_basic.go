package database

import (
	"strconv"
	"strings"
	"time"
)

func (c *client) cmdPing(args []string) []byte {
	if len(args) != 0 {
		return simpleError("wrong number of arguments for 'PING' command")
	}
	return simpleString("PONG")
}

func (c *client) cmdEcho(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'ECHO' command")
	}
	return bulkString(args[0], true)
}

func (c *client) cmdSet(args []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	if len(args) != 2 && len(args) != 4 {
		return simpleError("wrong number of arguments for 'SET' command")
	}
	key := args[0]
	value := args[1]
	var expiresAt time.Time

	if len(args) == 4 {
		option := strings.ToLower(args[2])
		expireValue := args[3]

		expireInt, err := strconv.Atoi(expireValue)
		if err != nil {
			return simpleError("invalid expiration value")
		}

		switch option {
		case "px":
			expiresAt = time.Now().Add(time.Duration(expireInt) * time.Millisecond)
		case "ex":
			expiresAt = time.Now().Add(time.Duration(expireInt) * time.Second)
		default:
			return simpleError("invalid expiration option")
		}
	}
	c.db.data[key] = dbEntry{
		value:     value,
		vType:     StringType,
		expiresAt: expiresAt,
	}
	return simpleString("OK")
}

func (c *client) cmdGet(args []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'GET' command")
	}
	content, _, exists, err := c.db.getStringEntry(args[0])
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		return bulkString("", false) // nil response
	}
	return bulkString(content, true)
}

func (c *client) cmdType(args []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'TYPE' command")
	}

	entry, exists := c.db.getEntry(args[0])
	if !exists {
		return simpleString("none")
	}

	switch entry.vType {
	case StringType:
		return simpleString("string")
	case ListType:
		return simpleString("list")
	case StreamType:
		return simpleString("stream")
	default:
		return simpleString("unknown")
	}
}

func (c *client) cmdIncr(args []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'INCR' command")
	}
	key := args[0]
	content, entry, exist, err := c.db.getStringEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exist {
		content = "0"
		entry = dbEntry{
			vType:     StringType,
			expiresAt: time.Time{},
		}
	}
	contentInt, err := strconv.Atoi(content)
	if err != nil {
		return simpleError(incrValueNotInteger)
	}
	contentInt++
	entry.value = strconv.Itoa(contentInt)
	c.db.data[key] = entry
	return respInteger(contentInt)
}
