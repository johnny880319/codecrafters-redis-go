package database

func (c *client) cmdSubscribe(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'SUBSCRIBE' command")
	}
	channel := args[0]

	c.db.rwMu.Lock()
	if _, exists := c.db.subscribers[channel]; !exists {
		c.db.subscribers[channel] = []*client{}
	}
	c.db.subscribers[channel] = append(c.db.subscribers[channel], c)

	subscribeCount := 0
	for _, subs := range c.db.subscribers {
		subscribeCount += len(subs)
	}
	c.db.rwMu.Unlock()
	return respArray([][]byte{
		bulkString("subscribe", true),
		bulkString(channel, true),
		respInteger(subscribeCount),
	})
}
