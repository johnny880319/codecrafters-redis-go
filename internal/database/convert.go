package database

import (
	"fmt"
	"strconv"
)

func parseCommand(buf []byte) ([]string, error) {
	if len(buf) == 0 || buf[0] != '*' {
		return nil, fmt.Errorf("invalid command format")
	}

	offset := 1
	// get number of arguments
	numArgs := 0
	for i := 1; buf[i] != '\r'; i++ {
		numArgs = numArgs*10 + int(buf[i]-'0')
		offset++
	}
	offset += 2 // skip \r\n

	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		if buf[offset] != '$' {
			return nil, fmt.Errorf("invalid argument format")
		}
		offset++

		argLen := 0
		for j := offset; buf[j] != '\r'; j++ {
			argLen = argLen*10 + int(buf[j]-'0')
			offset++
		}
		offset += 2 // skip \r\n

		args[i] = string(buf[offset : offset+argLen])
		offset += argLen + 2 // skip argument and \r\n
	}
	return args, nil
}

func simpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

func bulkString(s string, exist bool) []byte {
	if !exist {
		return []byte("$-1\r\n") // null bulk string
	}
	return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

func respInteger(i int) []byte {
	return []byte(":" + strconv.Itoa(i) + "\r\n")
}

func respArray(arr [][]byte) []byte {
	if arr == nil {
		return []byte("*-1\r\n") // null array
	}
	result := []byte("*" + strconv.Itoa(len(arr)) + "\r\n")
	for _, elem := range arr {
		result = append(result, elem...)
	}
	return result
}

func simpleError(msg string) []byte {
	return []byte("-ERR " + msg + "\r\n")
}

func (db *Database) getStringEntry(key string) (string, dbEntry, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return "", dbEntry{}, false, nil
	}
	if entry.vType != StringType {
		return "", dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
	}
	if content, ok := entry.value.(string); ok {
		return content, entry, true, nil
	}
	return "", dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
}

func (db *Database) getListEntry(key string) ([]string, dbEntry, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return nil, dbEntry{}, false, nil
	}
	if entry.vType != ListType {
		return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
	}
	if content, ok := entry.value.([]string); ok {
		return content, entry, true, nil
	}
	return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
}

func (db *Database) getStreamEntry(key string) ([]map[string]string, dbEntry, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return nil, dbEntry{}, false, nil
	}
	if entry.vType != StreamType {
		return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
	}
	if content, ok := entry.value.([]map[string]string); ok {
		return content, entry, true, nil
	}
	return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
}
