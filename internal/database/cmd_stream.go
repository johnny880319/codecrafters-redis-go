package database

import (
	"strings"
	"time"
)

func (db *Database) cmdType(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'TYPE' command\r\n")
	}
	key := args[0]

	entry, exists := db.data[key]
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

func (db *Database) cmdXadd(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) < 2 || len(args)%2 == 1 {
		return []byte("-ERR wrong number of arguments for 'XADD' command\r\n")
	}
	key := args[0]
	id := args[1]
	fields := args[2:]

	if id == "0-0" {
		return simpleError("The ID specified in XADD must be greater than 0-0")
	}

	entry, exists := db.data[key]
	if !exists {
		entry = dbEntry{
			value:     []map[string]string{},
			vType:     StreamType,
			expiresAt: time.Time{},
		}
	} else if entry.vType != StreamType {
		return []byte("-ERR wrong type of value for 'XADD' command\r\n")
	}

	stream := entry.value.([]map[string]string)

	if len(stream) > 0 {
		lastID := stream[len(stream)-1]["id"]
		lastDash := strings.Index(lastID, "-")
		newDash := strings.Index(id, "-")
		if lastDash == -1 || newDash == -1 {
			return simpleError("Invalid ID format for XADD")
		}
		lastTimestamp := lastID[:lastDash]
		lastSequence := lastID[lastDash+1:]
		newTimestamp := id[:newDash]
		newSequence := id[newDash+1:]

		if newTimestamp < lastTimestamp || (newTimestamp == lastTimestamp && newSequence <= lastSequence) {
			return simpleError("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	newEntry := make(map[string]string)
	newEntry["id"] = id
	for i := 0; i < len(fields); i += 2 {
		newEntry[fields[i]] = fields[i+1]
	}
	stream = append(stream, newEntry)
	entry.value = stream
	db.data[key] = entry
	return bulkString(id, true)
}
