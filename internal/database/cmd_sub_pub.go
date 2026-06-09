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

func (c *client) cmdUnsubscribe(args []string) []byte {
	if len(args) != 1 {
		return simpleError("wrong number of arguments for 'UNSUBSCRIBE' command")
	}
	channel := args[0]

	c.db.rwMu.Lock()
	if subscribers, exists := c.db.subscribers[channel]; exists {
		delete(subscribers, c)
		if len(subscribers) == 0 {
			delete(c.db.subscribers, channel)
		}
	}
	delete(c.subscribedChannels, channel)
	c.db.rwMu.Unlock()

	return respArray([][]byte{
		bulkString("unsubscribe", true),
		bulkString(channel, true),
		respInteger(len(c.subscribedChannels)),
	})
}

func (c *client) cmdPublish(args []string) []byte {
	if len(args) != 2 {
		return simpleError("wrong number of arguments for 'PUBLISH' command")
	}
	channel := args[0]
	message := args[1]
	subs := make([]*client, 0)

	c.db.rwMu.RLock()
	for subscriber := range c.db.subscribers[channel] {
		subs = append(subs, subscriber)
	}
	c.db.rwMu.RUnlock()

	for _, sub := range subs {
		response := respArray([][]byte{
			bulkString("message", true),
			bulkString(channel, true),
			bulkString(message, true),
		})
		_, err := sub.conn.Write(response)
		if err != nil {
			return simpleError("error publishing message")
		}
	}
	return respInteger(len(subs))
}
