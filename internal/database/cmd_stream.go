package database

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

func (c *client) cmdXadd(args []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	if len(args) < 2 || len(args)%2 == 1 {
		return simpleError("wrong number of arguments for 'XADD' command")
	}
	key := args[0]
	id := args[1]
	fields := args[2:]

	content, entry, exists, err := c.db.getStreamEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		entry = dbEntry{
			value:     []map[string]string{},
			vType:     StreamType,
			expiresAt: time.Time{},
		}
	}

	id, errMsg := handleXaddId(id, content)
	if errMsg != "" {
		return simpleError(errMsg)
	}

	newEntry := make(map[string]string)
	newEntry["id"] = id
	for i := 0; i < len(fields); i += 2 {
		newEntry[fields[i]] = fields[i+1]
	}
	content = append(content, newEntry)
	entry.value = content
	c.db.data[key] = entry

	c.db.notifyWaiters(key)
	return bulkString(id, true)
}

func handleXaddId(rawId string, stream []map[string]string) (finishedId string, errMsg string) {
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

func (c *client) cmdXrange(args []string) []byte {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()

	if len(args) != 3 {
		return simpleError("wrong number of arguments for 'XRANGE' command")
	}
	key := args[0]
	start := args[1]
	stop := args[2]

	content, _, exists, err := c.db.getStreamEntry(key)
	if err != nil {
		return simpleError(err.Error())
	}
	if !exists {
		return respArray([][]byte{}) // empty stream
	}

	results := [][]byte{}
	for _, item := range content {
		id := item["id"]
		compare1, err1 := compareIds(start, id)
		compare2, err2 := compareIds(id, stop)
		if err1 != nil || err2 != nil {
			return simpleError("invalid ID format for 'XRANGE' command")
		}
		if compare1 <= 0 && compare2 <= 0 {
			values := [][]byte{}
			for k, v := range item {
				if k != "id" {
					values = append(values, bulkString(k, true), bulkString(v, true))
				}
			}
			entry := respArray([][]byte{bulkString(id, true), respArray(values)})
			results = append(results, entry)
		}
	}
	return respArray(results)
}

func (c *client) cmdXread(args []string) []byte {
	if len(args) < 3 {
		return simpleError("wrong number of arguments for 'XREAD' command")
	}
	var err error
	timeoutMilisec := -1
	if strings.ToLower(args[0]) == "block" {
		timeoutMilisec, err = strconv.Atoi(args[1])
		if err != nil || timeoutMilisec < 0 {
			return simpleError("invalid BLOCK value for 'XREAD' command")
		}
		args = args[2:]
	}

	if strings.ToLower(args[0]) != "streams" {
		return simpleError("invalid syntax for 'XREAD' command")
	}
	args = args[1:]

	result := c.db.xreadOnce(args)
	if result != nil || timeoutMilisec == -1 {
		return result
	}

	args, err = c.db.resolveXreadArgs(args)
	if err != nil {
		return simpleError(err.Error())
	}

	waiter := make(chan string)
	c.db.addWaiter(waiter, args)

	if timeoutMilisec == 0 {
		<-waiter
		return c.db.xreadOnce(args)
	}

	select {
	case <-waiter:
		return c.db.xreadOnce(args)
	case <-time.After(time.Duration(timeoutMilisec) * time.Millisecond):
		return respArray(nil) // empty stream on timeout
	}
}

//nolint:gocognit // will refactor later
func (db *Database) xreadOnce(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args)%2 != 0 {
		return simpleError("wrong number of arguments for 'XREAD' command")
	}
	keys, ids := args[:len(args)/2], args[len(args)/2:]

	results := [][]byte{}
	for i := 0; i < len(keys); i++ {
		key, id := keys[i], ids[i]

		content, _, exists, err := db.getStreamEntry(key)
		if err != nil {
			return simpleError(err.Error())
		}
		if !exists {
			continue
		}

		for _, item := range content {
			itemId := item["id"]
			compare, err := compareIds(id, itemId)
			if err != nil {
				return simpleError("invalid ID format for 'XREAD' command")
			}
			if compare < 0 {
				values := [][]byte{}
				for k, v := range item {
					if k != "id" {
						values = append(values, bulkString(k, true), bulkString(v, true))
					}
				}
				entry := respArray([][]byte{bulkString(itemId, true), respArray(values)})
				entry = respArray([][]byte{entry})
				results = append(results, respArray([][]byte{bulkString(key, true), entry}))
				break
			}
		}
	}
	if len(results) == 0 {
		return nil
	}
	return respArray(results)
}

func (db *Database) resolveXreadArgs(args []string) ([]string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args)%2 != 0 {
		return nil, errors.New("wrong number of arguments for 'XREAD' command")
	}
	keys, ids := args[:len(args)/2], args[len(args)/2:]

	for i := 0; i < len(keys); i++ {
		key, id := keys[i], ids[i]

		content, _, exists, err := db.getStreamEntry(key)
		if err != nil {
			return nil, err
		}
		if !exists {
			if id == "$" {
				args[i+len(keys)] = "0-0"
			}
			continue
		}

		if id == "$" {
			id = content[len(content)-1]["id"]
			args[i+len(keys)] = id
			continue
		}
	}
	return args, nil
}

func compareIds(id1, id2 string) (int, error) {
	if id1 == "-" {
		return -1, nil
	}
	if id2 == "+" {
		return -1, nil
	}
	if id1 == "$" {
		return 1, nil
	}
	dash1 := strings.Index(id1, "-")
	dash2 := strings.Index(id2, "-")
	if dash1 == -1 || dash2 == -1 {
		dash1, dash2 = len(id1), len(id2)
	}
	timestamp1, err1 := strconv.Atoi(id1[:dash1])
	timestamp2, err2 := strconv.Atoi(id2[:dash2])
	if err1 != nil || err2 != nil {
		return 0, errors.New("invalid ID format")
	}
	if timestamp1 != timestamp2 {
		return timestamp1 - timestamp2, nil
	}
	if dash1 == len(id1) || dash2 == len(id2) {
		return 0, nil
	}
	sequence1, err1 := strconv.Atoi(id1[dash1+1:])
	sequence2, err2 := strconv.Atoi(id2[dash2+1:])
	if err1 != nil || err2 != nil {
		return 0, errors.New("invalid ID format")
	}
	return sequence1 - sequence2, nil
}

func (db *Database) addWaiter(waiter chan string, args []string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	keys := args[:len(args)/2]

	keysSet := make(map[string]struct{})
	for _, key := range keys {
		keysSet[key] = struct{}{}
	}

	for key := range keysSet {
		db.waiters[key] = append(db.waiters[key], waiter)
	}
}

func (db *Database) notifyWaiters(key string) {
	waiters, exists := db.waiters[key]
	if !exists {
		return
	}
	for _, waiter := range waiters {
		select {
		case waiter <- "":
		default:
		}
	}
	delete(db.waiters, key)
}
