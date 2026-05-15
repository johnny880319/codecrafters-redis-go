package database

func (c *client) cmdMulti(args []string) []byte {
	if len(args) != 0 {
		return simpleError("wrong number of arguments for 'MULTI' command")
	}
	if c.isMulti {
		return simpleError("MULTI calls can not be nested")
	}
	c.isMulti = true
	return simpleString("OK")
}

func (c *client) cmdExec(args []string) []byte {
	if len(args) != 0 {
		return simpleError("wrong number of arguments for 'EXEC' command")
	}
	if !c.isMulti {
		return simpleError(execWithoutMulti)
	}

	for key, watchedValue := range c.watched {
		c.db.mu.Lock()
		currentValue, _, _, err := c.db.getStringEntry(key)
		c.db.mu.Unlock()
		if err != nil {
			return simpleError(err.Error())
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
		return simpleError("wrong number of arguments for 'DISCARD' command")
	}
	if !c.isMulti {
		return simpleError(discardWithoutMulti)
	}
	c.isMulti = false
	c.cmdQueue = nil
	c.watched = make(map[string]string)
	return simpleString("OK")
}

func (c *client) cmdWatch(args []string) []byte {
	if c.isMulti {
		return simpleError(watchInsideMulti)
	}

	for _, key := range args {
		c.db.mu.Lock()
		content, _, _, err := c.db.getStringEntry(key)
		c.db.mu.Unlock()
		if err != nil {
			return simpleError(err.Error())
		}
		c.watched[key] = content
	}
	return simpleString("OK")
}

func (c *client) cmdUnwatch(args []string) []byte {
	if len(args) != 0 {
		return simpleError("wrong number of arguments for 'UNWATCH' command")
	}
	c.watched = make(map[string]string)
	return simpleString("OK")
}
