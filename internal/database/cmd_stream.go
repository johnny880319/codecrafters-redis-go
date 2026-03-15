package database

import (
	"strconv"
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

	id, errMsg := handleXaddId(id, stream)
	if errMsg != "" {
		return simpleError(errMsg)
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

func handleXaddId(rawId string, stream []map[string]string) (finished_id string, errMsg string) {
	if rawId == "0-0" {
		return "", xaddIDNotGreaterThanZero
	}
	lastTimestamp, lastSequence := 0, 0
	var err1, err2 error

	if len(stream) > 0 {
		lastId := stream[len(stream)-1]["id"]
		lastDash := strings.Index(lastId, "-")
		if lastDash == -1 {
			return "", "Invalid ID format for XADD"
		}
		lastTimestamp, err1 = strconv.Atoi(lastId[:lastDash])
		lastSequence, err2 = strconv.Atoi(lastId[lastDash+1:])
		if err1 != nil || err2 != nil {
			return "", "Invalid ID format for XADD"
		}
	}

	if rawId == "*" {
		rawTimestamp := max(lastTimestamp, int(time.Now().UnixMilli()))
		rawSequence := 0
		if rawTimestamp == lastTimestamp {
			rawSequence = lastSequence + 1
		}
		return strconv.Itoa(rawTimestamp) + "-" + strconv.Itoa(rawSequence), ""
	}
	rawDash := strings.Index(rawId, "-")
	if rawDash == -1 {
		return "", "Invalid ID format for XADD"
	}
	rawTimestamp, err := strconv.Atoi(rawId[:rawDash])
	if err != nil {
		return "", "Invalid ID format for XADD"
	}
	if rawTimestamp < lastTimestamp {
		return "", xaddIDNotGreaterThanLast
	}
	if rawId[rawDash+1:] == "*" {
		rawSequence := 0
		if rawTimestamp == lastTimestamp {
			rawSequence = lastSequence + 1
		}
		return strconv.Itoa(rawTimestamp) + "-" + strconv.Itoa(rawSequence), ""
	}
	rawSequence, err := strconv.Atoi(rawId[rawDash+1:])
	if err != nil {
		return "", "Invalid ID format for XADD"
	}
	if rawTimestamp == lastTimestamp && rawSequence <= lastSequence {
		return "", xaddIDNotGreaterThanLast
	}
	return rawId, ""
}
