package database

import "time"

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
