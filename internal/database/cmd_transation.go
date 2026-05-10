package database

import "strconv"

func (db *Database) cmdIncr(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'INCR' command")
	}
	key := args[0]
	content, entry, _, err := db.getStringEntry(key)
	if err != nil {
		return simpleError(err.Error())
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
