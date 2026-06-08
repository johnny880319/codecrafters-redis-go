package database

import (
	"sort"
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

func (c *client) cmdZrank(args []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

	if len(args) != 2 {
		return simpleError("wrong number of arguments for 'ZRANK' command")
	}
	key := args[0]
	member := args[1]

	content, _, exists, err := c.db.getSortedSetEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		return bulkString("", false)
	}

	content_slice := sortedSetToRespArray(content)

	for i, item := range content_slice {
		if item.member == member {
			return respInteger(i)
		}
	}

	return bulkString("", false)
}

func (c *client) cmdZrange(args []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

	if len(args) != 3 {
		return simpleError("wrong number of arguments for 'ZRANGE' command")
	}
	key := args[0]
	startStr := args[1]
	stopStr := args[2]

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return simpleError("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(stopStr)
	if err != nil {
		return simpleError("ERR value is not an integer or out of range")
	}

	content, _, exists, err := c.db.getSortedSetEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		return respArray([][]byte{})
	}

	content_slice := sortedSetToRespArray(content)

	length := len(content_slice)
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	start = max(start, 0)
	stop = min(stop, length-1)

	bytesValues := make([][]byte, 0)
	for i := start; i <= stop; i++ {
		bytesValues = append(bytesValues, bulkString(content_slice[i].member, true))
	}
	return respArray(bytesValues)
}

func sortedSetToRespArray(content map[string]float64) []struct {
	member string
	score  float64
} {
	content_slice := make([]struct {
		member string
		score  float64
	}, 0, len(content))
	for member, score := range content {
		content_slice = append(content_slice, struct {
			member string
			score  float64
		}{
			member,
			score,
		})
	}

	sort.Slice(content_slice, func(i, j int) bool {
		if content_slice[i].score == content_slice[j].score {
			return content_slice[i].member < content_slice[j].member
		}
		return content_slice[i].score < content_slice[j].score
	})
	return content_slice
}
