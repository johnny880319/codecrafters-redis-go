package database

import (
	"slices"
	"strconv"
	"time"
)

func (db *Database) cmdRpush(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) < 2 {
		return simpleError("wrong number of arguments for 'RPUSH' command")
	}
	key := args[0]
	values := args[1:]

	content, entry, exists, err := db.getListEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		entry = dbEntry{
			value:     []string{},
			vType:     ListType,
			expiresAt: time.Time{},
		}
	}

	content = append(content, values...)
	entry.value = content
	db.data[key] = entry
	newLen := len(content)

	if waiters, hasWaiters := db.waiters[key]; hasWaiters {
		for len(waiters) > 0 && len(content) > 0 {
			waiters[0] <- content[0]
			waiters = waiters[1:]
			content = content[1:]
		}
		if len(waiters) == 0 {
			delete(db.waiters, key)
		} else {
			db.waiters[key] = waiters
		}
		entry.value = content
		db.data[key] = entry
	}

	return respInteger(newLen)
}

func (db *Database) cmdLpush(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) < 2 {
		return simpleError("wrong number of arguments for 'LPUSH' command")
	}
	key := args[0]
	values := args[1:]

	content, entry, exists, err := db.getListEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		entry = dbEntry{
			value:     []string{},
			vType:     ListType,
			expiresAt: time.Time{},
		}
	}

	// reverse values before prepending
	slices.Reverse(values)
	content = slices.Insert(content, 0, values...)
	entry.value = content
	db.data[key] = entry
	newLen := len(content)

	if waiters, hasWaiters := db.waiters[key]; hasWaiters {
		values := content
		for len(waiters) > 0 && len(values) > 0 {
			waiters[0] <- values[0]
			waiters = waiters[1:]
			values = values[1:]
		}
		if len(waiters) == 0 {
			delete(db.waiters, key)
		} else {
			db.waiters[key] = waiters
		}
		entry.value = values
		db.data[key] = entry
	}

	return respInteger(newLen)
}

func (db *Database) cmdLpop(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) < 1 || len(args) > 2 {
		return simpleError("wrong number of arguments for 'LPOP' command")
	}
	key := args[0]

	content, entry, exists, err := db.getListEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists || len(content) == 0 {
		return bulkString("", false) // nil response
	}

	if len(args) == 1 {
		value := content[0]
		entry.value = content[1:] // remove first element
		db.data[key] = entry
		return bulkString(value, true)
	}

	count, err := strconv.Atoi(args[1])
	if err != nil || count < 0 {
		return simpleError("invalid count value")
	}
	count = min(count, len(content))

	values := content[:count]
	entry.value = content[count:] // remove popped elements
	db.data[key] = entry

	bytesValues := make([][]byte, len(values))
	for i, v := range values {
		bytesValues[i] = bulkString(v, true)
	}
	return respArray(bytesValues)
}

func (db *Database) cmdBLpop(args []string) []byte {
	if len(args) != 2 {
		return simpleError("wrong number of arguments for 'BLPOP' command")
	}
	key := args[0]
	timeoutSec, err := strconv.ParseFloat(args[1], 64)
	if err != nil || timeoutSec < 0 {
		return simpleError("invalid timeout value")
	}

	var waiter chan string
	var response []byte

	func() {
		db.mu.Lock()
		defer db.mu.Unlock()

		content, entry, exists, err := db.getListEntry(key)
		if err != nil {
			response = simpleError(err.Error())
			return
		}
		if !exists {
			entry = dbEntry{
				value:     []string{},
				vType:     ListType,
				expiresAt: time.Time{},
			}
			db.data[key] = entry
		}

		if len(content) > 0 {
			value := content[0]
			entry.value = content[1:] // remove first element
			db.data[key] = entry
			response = respArray([][]byte{bulkString(key, true), bulkString(value, true)})
			return
		}

		waiter = make(chan string)
		db.waiters[key] = append(db.waiters[key], waiter)
	}()

	if response != nil {
		return response
	}
	if timeoutSec == 0 {
		value := <-waiter
		return respArray([][]byte{bulkString(key, true), bulkString(value, true)})
	}
	timer := time.NewTimer(time.Duration(timeoutSec * float64(time.Second)))
	select {
	case value := <-waiter:
		return respArray([][]byte{bulkString(key, true), bulkString(value, true)})
	case <-timer.C:
		db.mu.Lock()
		defer db.mu.Unlock()
		// Remove waiter from the list
		waiters := db.waiters[key]
		for i, w := range waiters {
			if w == waiter {
				db.waiters[key] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(db.waiters[key]) == 0 {
			delete(db.waiters, key)
		}
		return respArray(nil) // nil on timeout
	}
}

func (db *Database) cmdLrange(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 3 {
		return simpleError("wrong number of arguments for 'LRANGE' command")
	}
	key := args[0]
	start, err1 := strconv.Atoi(args[1])
	stop, err2 := strconv.Atoi(args[2])
	if err1 != nil || err2 != nil {
		return simpleError("invalid start or stop index")
	}

	content, _, exists, err := db.getListEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		return respArray([][]byte{}) // empty list
	}

	length := len(content)

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	start = max(start, 0)
	stop = min(stop, length-1)
	bytesValues := make([][]byte, 0)
	for i := start; i <= stop; i++ {
		bytesValues = append(bytesValues, bulkString(content[i], true))
	}
	return respArray(bytesValues)
}

func (db *Database) cmdLlen(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'LLEN' command")
	}
	key := args[0]

	content, _, exists, err := db.getListEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		return respInteger(0) // empty list
	}

	return respInteger(len(content))
}
