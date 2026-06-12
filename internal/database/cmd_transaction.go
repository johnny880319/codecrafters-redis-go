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

	for key, watchedVersion := range c.watchedVersion {
		c.db.rwMu.Lock()
		_, _ = c.db.getEntry(key) // update version for the key if expired
		currentVersion := c.db.versions[key]
		c.db.rwMu.Unlock()
		if watchedVersion != currentVersion {
			c.isMulti = false
			c.cmdQueue = nil
			c.watchedVersion = make(map[string]int)
			return respArray(nil)
		}
	}

	responses := make([][]byte, len(c.cmdQueue))
	for i, ctx := range c.cmdQueue {
		responses[i] = c.executeCommand(ctx.command)
	}
	c.isMulti = false
	c.cmdQueue = nil
	c.watchedVersion = make(map[string]int)
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
	c.watchedVersion = make(map[string]int)
	return simpleString("OK")
}

func (c *client) cmdWatch(args []string) []byte {
	c.db.rwMu.Lock()
	defer c.db.rwMu.Unlock()

	if c.isMulti {
		return simpleError(redisErr, watchInsideMulti)
	}

	for _, key := range args {
		_, _ = c.db.getEntry(key) // update version for the key if expired
		c.watchedVersion[key] = c.db.versions[key]
	}
	return simpleString("OK")
}

func (c *client) cmdUnwatch(args []string) []byte {
	if len(args) != 0 {
		return simpleError(redisErr, "usage: UNWATCH")
	}
	c.watchedVersion = make(map[string]int)
	return simpleString("OK")
}
