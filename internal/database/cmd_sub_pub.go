package database

func (c *client) cmdSubscribe(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'SUBSCRIBE' command")
	}
	channel := args[0]

	c.db.rwMu.Lock()
	if _, exists := c.db.subscribers[channel]; !exists {
		c.db.subscribers[channel] = make(map[*client]struct{})
	}
	c.db.subscribers[channel][c] = struct{}{}
	c.subscribedChannels[channel] = struct{}{}

	c.db.rwMu.Unlock()
	return respArray([][]byte{
		bulkString("subscribe", true),
		bulkString(channel, true),
		respInteger(len(c.subscribedChannels)),
	})
}
