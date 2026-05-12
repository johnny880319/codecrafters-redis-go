package database

import (
	"strconv"
	"time"
)

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

func (c *client) cmdMulti(_ []string) []byte {
	c.isMulti = true
	return simpleString("OK")
}

func (c *client) cmdExec(_ []string) []byte {
	if !c.isMulti {
		return simpleError(execWithoutMulti)
	}
	responses := make([][]byte, len(c.cmdQueue))
	for i, command := range c.cmdQueue {
		responses[i] = c.executeCommand(command[0], command[1:])
	}
	c.isMulti = false
	c.cmdQueue = nil
	return respArray(responses)
}
