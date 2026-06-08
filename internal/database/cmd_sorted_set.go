package database

import (
	"strconv"
	"time"
)

func (c *client) cmdZadd(args []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

	if len(args) != 3 {
		return simpleError("wrong number of arguments for 'ZADD' command")
	}
	key := args[0]
	scoreStr := args[1]
	member := args[2]

	score, err := strconv.ParseFloat(scoreStr, 64)
	if err != nil {
		return simpleError("ERR value is not a valid float")
	}

	content, entry, exists, err := c.db.getSortedSetEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		entry = dbEntry{
			value:     make(map[string]float64),
			vType:     SortedSetType,
			expiresAt: time.Time{},
		}
	}

	returnVal := 1
	if _, memberExists := content[member]; memberExists {
		returnVal = 0
	}

	content[member] = score
	entry.value = content
	c.db.data[key] = entry

	return respInteger(returnVal)
}
