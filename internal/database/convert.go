package database

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func readCommand(reader *bufio.Reader) ([]string, error) {
	line, err := readRespLine(reader)
	if err != nil {
		return nil, err
	}

	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("invalid command format")
	}

	numArgs, err := strconv.Atoi(line[1:])
	if err != nil || numArgs < 0 {
		return nil, fmt.Errorf("invalid number of arguments: %s", line[1:])
	}

	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err := readRespLine(reader)
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != '$' {
			return nil, fmt.Errorf("invalid argument format")
		}

		argLen, err := strconv.Atoi(line[1:])
		if err != nil || argLen < 0 {
			return nil, fmt.Errorf("invalid argument length: %s", line[1:])
		}

		buf := make([]byte, argLen+2) // +2 for \r\n
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(string(buf), "\r\n") {
			return nil, fmt.Errorf("invalid argument format: missing CRLF")
		}

		args[i] = string(buf[:argLen])
	}
	return args, nil
}

func readRespLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("invalid RESP line: %s", line)
	}
	return strings.TrimSuffix(line, "\r\n"), nil
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
