package database

import (
	"strconv"
	"time"
)

func (c *client) cmdGeoadd(args []string) []byte {
	if len(args) != 4 {
		return simpleError("wrong number of arguments for 'GEOADD' command")
	}
	key := args[0]
	member := args[3]
	longitude, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return simpleError("invalid longitude value")
	}
	latitude, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return simpleError("invalid latitude value")
	}

	if longitude < -180 || longitude > 180 || latitude < -85.05112878 || latitude > 85.05112878 {
		return simpleError(invalidLongitudeLatitude)
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

	content[member] = 0
	entry.value = content
	c.db.data[key] = entry

	return respInteger(returnVal)
}
