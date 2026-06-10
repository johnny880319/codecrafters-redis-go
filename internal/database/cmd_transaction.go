package database

func (c *client) cmdMulti(args []string) []byte {
	if len(args) != 0 {
		return simpleError(redisErr, "usage: MULTI")
	}
	if c.isMulti {
		return simpleError(redisErr, "MULTI calls can not be nested")
	}
	c.isMulti = true
	return simpleString("OK")
}

func (c *client) cmdExec(args []string) []byte {
	if len(args) != 0 {
		return simpleError(redisErr, "usage: EXEC")
	}
	if !c.isMulti {
		return simpleError(redisErr, execWithoutMulti)
	}

	for key, watchedValue := range c.watched {
		c.db.rwMu.Lock()
		currentValue, _, _, err := c.db.getStringEntry(key)
		c.db.rwMu.Unlock()
		if err != nil {
			return simpleError(redisErr, err.Error())
		}
		if currentValue != watchedValue {
			c.isMulti = false
			c.cmdQueue = nil
			c.watched = make(map[string]string)
			return respArray(nil)
		}
	}

	responses := make([][]byte, len(c.cmdQueue))
	for i, command := range c.cmdQueue {
		responses[i] = c.executeCommand(command)
	}
	c.isMulti = false
	c.cmdQueue = nil
	c.watched = make(map[string]string)
	return respArray(responses)
}

func (c *client) cmdDiscard(args []string) []byte {
	if len(args) != 0 {
		return simpleError(redisErr, "usage: DISCARD")
	}
	if !c.isMulti {
		return simpleError(redisErr, discardWithoutMulti)
	}
	c.isMulti = false
	c.cmdQueue = nil
	c.watched = make(map[string]string)
	return simpleString("OK")
}

func (c *client) cmdWatch(args []string) []byte {
	if c.isMulti {
		return simpleError(redisErr, watchInsideMulti)
	}

	for _, key := range args {
		c.db.rwMu.Lock()
		content, _, _, err := c.db.getStringEntry(key)
		c.db.rwMu.Unlock()
		if err != nil {
			return simpleError(redisErr, err.Error())
		}
		c.watched[key] = content
	}
	return simpleString("OK")
}

func (c *client) cmdUnwatch(args []string) []byte {
	if len(args) != 0 {
		return simpleError(redisErr, "usage: UNWATCH")
	}
	c.watched = make(map[string]string)
	return simpleString("OK")
}
