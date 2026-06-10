package database

import "strings"

const (
	redisErr       = "ERR"
	redisWrongPass = "WRONGPASS"
	redisNoAuth    = "NOAUTH"
)

const (
	xaddIDNotGreaterThanZero = "The ID specified in XADD must be greater than 0-0"
	xaddIDNotGreaterThanLast = "The ID specified in XADD is equal or smaller than the target stream top item"
	incrValueNotInteger      = "value is not an integer or out of range"
	execWithoutMulti         = "EXEC without MULTI"
	discardWithoutMulti      = "DISCARD without MULTI"
	watchInsideMulti         = "WATCH inside MULTI is not allowed"
	invalidLongitudeLatitude = "invalid longitude,latitude pair"
	wrongPass                = "invalid username-password pair or user is disabled."
	noAuth                   = "Authentication required."
)

func executeInSubscribeModeError(command string) string {
	return "Can't execute '" + strings.ToLower(command) +
		"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
}
