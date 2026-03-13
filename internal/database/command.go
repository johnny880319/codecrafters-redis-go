package database

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

func (db *Database) getCommandMap() map[string]func(args []string) []byte {
	return map[string]func(args []string) []byte{
		"ping":   db.cmdPing,
		"echo":   db.cmdEcho,
		"set":    db.cmdSet,
		"get":    db.cmdGet,
		"rpush":  db.cmdRpush,
		"lpush":  db.cmdLpush,
		"lpop":   db.cmdLpop,
		"blpop":  db.cmdBLpop,
		"lrange": db.cmdLrange,
		"llen":   db.cmdLlen,
	}
}

func (db *Database) cmdPing(args []string) []byte {
	if len(args) != 0 {
		return []byte("-ERR wrong number of arguments for 'PING' command\r\n")
	}
	return []byte("+PONG\r\n")
}

func (db *Database) cmdEcho(args []string) []byte {
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'ECHO' command\r\n")
	}
	return []byte("$" + strconv.Itoa(len(args[0])) + "\r\n" + args[0] + "\r\n")
}

func (db *Database) cmdSet(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 2 && len(args) != 4 {
		return []byte("-ERR wrong number of arguments for 'SET' command\r\n")
	}
	key := args[0]
	value := args[1]
	var expiresAt time.Time

	if len(args) == 4 {
		option := strings.ToLower(args[2])
		expireValue := args[3]

		expireInt, err := strconv.Atoi(expireValue)
		if err != nil {
			return []byte("-ERR invalid expiration value\r\n")
		}

		switch option {
		case "px":
			expiresAt = time.Now().Add(time.Duration(expireInt) * time.Millisecond)
		case "ex":
			expiresAt = time.Now().Add(time.Duration(expireInt) * time.Second)
		default:
			return []byte("-ERR invalid expiration option\r\n")
		}
	}
	db.data[key] = dbEntry{
		value:     value,
		vType:     StringType,
		expiresAt: expiresAt,
	}
	return []byte("+OK\r\n")
}

func (db *Database) cmdGet(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'GET' command\r\n")
	}
	key := args[0]
	val, ok := db.data[key]
	if !ok {
		return []byte("$-1\r\n") // nil response
	}
	// check if the key has expired
	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		delete(db.data, key)
		return []byte("$-1\r\n") // nil response
	}
	return []byte("$" + strconv.Itoa(len(val.value.(string))) + "\r\n" + val.value.(string) + "\r\n")
}

func (db *Database) cmdRpush(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments for 'RPUSH' command\r\n")
	}
	key := args[0]
	values := args[1:]

	entry, exists := db.data[key]
	if !exists {
		entry = dbEntry{
			value:     []string{},
			vType:     ListType,
			expiresAt: time.Time{},
		}
	} else if entry.vType != ListType {
		return []byte("-ERR wrong type of value for 'RPUSH' command\r\n")
	}

	entry.value = append(entry.value.([]string), values...)
	db.data[key] = entry

	if waiters, hasWaiters := db.waiters[key]; hasWaiters {
		values := entry.value.([]string)
		for len(waiters) > 0 && len(values) > 0 {
			waiter := waiters[0]
			waiters = waiters[1:]

			value := values[0]
			values = values[1:] // remove first element

			waiter <- value
		}
		if len(waiters) == 0 {
			delete(db.waiters, key)
		} else {
			db.waiters[key] = waiters
		}
		entry.value = values
		db.data[key] = entry
	}

	return []byte(":" + strconv.Itoa(len(entry.value.([]string))) + "\r\n")
}

func (db *Database) cmdLpush(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments for 'LPUSH' command\r\n")
	}
	key := args[0]
	values := args[1:]

	entry, exists := db.data[key]
	if !exists {
		entry = dbEntry{
			value:     []string{},
			vType:     ListType,
			expiresAt: time.Time{},
		}
	} else if entry.vType != ListType {
		return []byte("-ERR wrong type of value for 'LPUSH' command\r\n")
	}

	// reverse values before prepending
	slices.Reverse(values)
	entry.value = slices.Insert(entry.value.([]string), 0, values...)
	db.data[key] = entry

	if waiters, hasWaiters := db.waiters[key]; hasWaiters {
		values := entry.value.([]string)
		for len(waiters) > 0 && len(values) > 0 {
			waiter := waiters[0]
			waiters = waiters[1:]

			value := values[0]
			values = values[1:] // remove first element

			waiter <- value
		}
		if len(waiters) == 0 {
			delete(db.waiters, key)
		} else {
			db.waiters[key] = waiters
		}
		entry.value = values
		db.data[key] = entry
	}

	return []byte(":" + strconv.Itoa(len(entry.value.([]string))) + "\r\n")
}

func (db *Database) cmdLpop(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) < 1 || len(args) > 2 {
		return []byte("-ERR wrong number of arguments for 'LPOP' command\r\n")
	}
	key := args[0]

	entry, exists := db.data[key]
	if !exists {
		return []byte("$-1\r\n") // nil response
	} else if entry.vType != ListType {
		return []byte("-ERR wrong type of value for 'LPOP' command\r\n")
	}

	list, ok := entry.value.([]string)
	if !ok || len(list) == 0 {
		return []byte("$-1\r\n") // nil response
	}

	if len(args) == 1 {
		value := list[0]
		entry.value = list[1:] // remove first element
		db.data[key] = entry
		return []byte("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
	}

	count, err := strconv.Atoi(args[1])
	if err != nil || count < 0 {
		return []byte("-ERR invalid count value\r\n")
	}
	count = min(count, len(list))

	values := list[:count]
	entry.value = list[count:] // remove popped elements
	db.data[key] = entry

	var response strings.Builder
	response.WriteString("*" + strconv.Itoa(len(values)) + "\r\n")
	for _, value := range values {
		response.WriteString("$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
	}
	return []byte(response.String())
}

func (db *Database) cmdBLpop(args []string) []byte {
	if len(args) != 2 {
		return []byte("-ERR wrong number of arguments for 'BLPOP' command\r\n")
	}
	key := args[0]
	timeoutSec, err := strconv.Atoi(args[1])
	if err != nil || timeoutSec < 0 {
		return []byte("-ERR invalid timeout value\r\n")
	}

	var waiter chan string
	var response []byte

	func() {
		db.mu.Lock()
		defer db.mu.Unlock()
		entry, exists := db.data[key]
		if !exists {
			entry = dbEntry{
				value:     []string{},
				vType:     ListType,
				expiresAt: time.Time{},
			}
			db.data[key] = entry
		} else if entry.vType != ListType {
			response = []byte("-ERR wrong type of value for 'BLPOP' command\r\n")
			return
		}

		list, ok := entry.value.([]string)
		if !ok {
			response = []byte("-ERR wrong type of value for 'BLPOP' command\r\n")
			return
		}

		if len(list) > 0 {
			value := list[0]
			entry.value = list[1:] // remove first element
			db.data[key] = entry
			response = []byte("*2" + "\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key +
				"\r\n$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
			return
		}

		waiter = make(chan string)
		db.waiters[key] = append(db.waiters[key], waiter)
	}()

	if response != nil {
		return response
	}
	value := <-waiter
	return []byte("*2" + "\r\n$" + strconv.Itoa(len(key)) + "\r\n" + key +
		"\r\n$" + strconv.Itoa(len(value)) + "\r\n" + value + "\r\n")
}

func (db *Database) cmdLrange(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 3 {
		return []byte("-ERR wrong number of arguments for 'LRANGE' command\r\n")
	}
	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return []byte("-ERR invalid start or stop index\r\n")
	}

	entry, exists := db.data[key]
	if !exists {
		return []byte("*0\r\n") // empty list
	} else if entry.vType != ListType {
		return []byte("-ERR wrong type of value for 'LRANGE' command\r\n")
	}

	list, ok := entry.value.([]string)
	if !ok {
		return []byte("-ERR wrong type of value for 'LRANGE' command\r\n")
	}
	length := len(list)

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return []byte("*0\r\n") // empty list
	}

	var response strings.Builder
	response.WriteString("*" + strconv.Itoa(stop-start+1) + "\r\n")
	for i := start; i <= stop; i++ {
		response.WriteString("$" + strconv.Itoa(len(list[i])) + "\r\n" + list[i] + "\r\n")
	}
	return []byte(response.String())
}

func (db *Database) cmdLlen(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'LLEN' command\r\n")
	}
	key := args[0]

	entry, exists := db.data[key]
	if !exists {
		return []byte(":0\r\n") // empty list
	} else if entry.vType != ListType {
		return []byte("-ERR wrong type of value for 'LLEN' command\r\n")
	}

	list, ok := entry.value.([]string)
	if !ok {
		return []byte("-ERR wrong type of value for 'LLEN' command\r\n")
	}
	return []byte(":" + strconv.Itoa(len(list)) + "\r\n")
}
