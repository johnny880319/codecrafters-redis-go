package database

import (
	"strconv"
	"strings"
	"time"
)

func (db *Database) cmdPing(args []string) string {
	if len(args) != 0 {
		return simpleError("wrong number of arguments for 'PING' command")
	}
	return simpleString("PONG")
}

func (db *Database) cmdEcho(args []string) string {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'ECHO' command")
	}
	return bulkString(args[0], true)
}

func (db *Database) cmdSet(args []string) string {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 2 && len(args) != 4 {
		return simpleError("wrong number of arguments for 'SET' command")
	}
	key := args[0]
	value := args[1]
	var expiresAt time.Time

	if len(args) == 4 {
		option := strings.ToLower(args[2])
		expireValue := args[3]

		expireInt, err := strconv.Atoi(expireValue)
		if err != nil {
			return simpleError("invalid expiration value")
		}

		switch option {
		case "px":
			expiresAt = time.Now().Add(time.Duration(expireInt) * time.Millisecond)
		case "ex":
			expiresAt = time.Now().Add(time.Duration(expireInt) * time.Second)
		default:
			return simpleError("invalid expiration option")
		}
	}
	db.data[key] = dbEntry{
		value:     value,
		vType:     StringType,
		expiresAt: expiresAt,
	}
	return simpleString("OK")
}

func (db *Database) cmdGet(args []string) string {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'GET' command")
	}
	key := args[0]
	val, ok := db.data[key]
	if !ok {
		return bulkString("", false) // nil response
	}
	// check if the key has expired
	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		delete(db.data, key)
		return bulkString("", false) // nil response
	}
	return bulkString(val.value.(string), true)
}
