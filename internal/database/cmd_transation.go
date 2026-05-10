package database

import (
	"strconv"
	"time"
)

func (db *Database) cmdIncr(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'INCR' command")
	}
	key := args[0]
	content, entry, exist, err := db.getStringEntry(key)
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
		return simpleError("value is not an integer or out of range")
	}
	contentInt++
	entry.value = strconv.Itoa(contentInt)
	db.data[key] = entry
	return respInteger(contentInt)
}
