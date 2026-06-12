package database

import "strings"

type commandContext struct {
	command         []string
	originalCommand []byte
}

type commandResult struct {
	response []byte
	applied  []byte
}

func isMutatingCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "SET", "INCR", "RPUSH", "LPUSH", "LPOP", "BLPOP", "XADD", "ZADD", "ZREM", "GEOADD":
		return true
	default:
		return false
	}
}

func isSubscribingCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT":
		return true
	default:
		return false
	}
}

func isMultiCommand(cmd string) bool {
	switch strings.ToUpper(cmd) {
	case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH":
		return true
	default:
		return false
	}
}

func (c *client) handleCommand(ctx commandContext) commandResult {
	cmd := ctx.command[0]
	if !c.hasAuthenticated && strings.ToUpper(cmd) != "AUTH" {
		return commandResult{response: simpleError(redisNoAuth, noAuth)}
	}

	if len(c.subscribedChannels) > 0 && !isSubscribingCommand(cmd) {
		return commandResult{response: simpleError(redisErr, executeInSubscribeModeError(cmd))}
	}

	if c.isMulti && !isMultiCommand(cmd) {
		c.cmdQueue = append(c.cmdQueue, ctx)
		return commandResult{response: simpleString("QUEUED")}
	}

	applied := []byte{}
	if strings.ToUpper(cmd) == "EXEC" {
		for _, queuedCmd := range c.cmdQueue {
			if isMutatingCommand(queuedCmd.command[0]) {
				applied = append(applied, queuedCmd.originalCommand...)
			}
		}
	} else if isMutatingCommand(cmd) {
		applied = append(applied, ctx.originalCommand...)
	}

	response := c.executeCommand(ctx.command)

	if len(response) == 0 || response[0] == '-' || (strings.ToUpper(cmd) == "EXEC" && string(response) == "*-1\r\n") {
		return commandResult{response: response}
	}

	return commandResult{response: response, applied: applied}
}

func (c *client) executeCommand(command []string) []byte {
	cmd, args := command[0], command[1:]
	switch strings.ToUpper(cmd) {
	case "PING":
		return c.cmdPing(args)
	case "ECHO":
		return c.cmdEcho(args)
	case "SET":
		return c.cmdSet(args)
	case "GET":
		return c.cmdGet(args)
	case "TYPE":
		return c.cmdType(args)
	case "INCR":
		return c.cmdIncr(args)
	case "CONFIG":
		return c.cmdConfig(args)
	case "KEYS":
		return c.cmdKeys(args)
	case "RPUSH":
		return c.cmdRpush(args)
	case "LPUSH":
		return c.cmdLpush(args)
	case "LPOP":
		return c.cmdLpop(args)
	case "BLPOP":
		return c.cmdBLpop(args)
	case "LRANGE":
		return c.cmdLrange(args)
	case "LLEN":
		return c.cmdLlen(args)
	case "XADD":
		return c.cmdXadd(args)
	case "XRANGE":
		return c.cmdXrange(args)
	case "XREAD":
		return c.cmdXread(args)
	case "MULTI":
		return c.cmdMulti(args)
	case "EXEC":
		return c.cmdExec(args)
	case "DISCARD":
		return c.cmdDiscard(args)
	case "WATCH":
		return c.cmdWatch(args)
	case "UNWATCH":
		return c.cmdUnwatch(args)
	case "INFO":
		return c.cmdInfo(args)
	case "REPLCONF":
		return c.cmdReplconf(args)
	case "PSYNC":
		return c.cmdPsync(args)
	case "WAIT":
		return c.cmdWait(args)
	case "SUBSCRIBE":
		return c.cmdSubscribe(args)
	case "UNSUBSCRIBE":
		return c.cmdUnsubscribe(args)
	case "PUBLISH":
		return c.cmdPublish(args)
	case "ZADD":
		return c.cmdZadd(args)
	case "ZRANK":
		return c.cmdZrank(args)
	case "ZRANGE":
		return c.cmdZrange(args)
	case "ZCARD":
		return c.cmdZcard(args)
	case "ZSCORE":
		return c.cmdZscore(args)
	case "ZREM":
		return c.cmdZrem(args)
	case "GEOADD":
		return c.cmdGeoadd(args)
	case "GEOPOS":
		return c.cmdGeopos(args)
	case "GEODIST":
		return c.cmdGeodist(args)
	case "GEOSEARCH":
		return c.cmdGeosearch(args)
	case "ACL":
		return c.cmdAcl(args)
	case "AUTH":
		return c.cmdAuth(args)
	default:
		return []byte("-ERR unknown command\r\n")
	}
}
