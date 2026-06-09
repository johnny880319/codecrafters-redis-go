package database

import "strings"

const (
	xaddIDNotGreaterThanZero = "ERR The ID specified in XADD must be greater than 0-0"
	xaddIDNotGreaterThanLast = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
	incrValueNotInteger      = "ERR value is not an integer or out of range"
	execWithoutMulti         = "ERR EXEC without MULTI"
	discardWithoutMulti      = "ERR DISCARD without MULTI"
	watchInsideMulti         = "ERR WATCH inside MULTI is not allowed"
	invalidLongitudeLatitude = "ERR invalid longitude,latitude pair"
	wrongPass                = "WRONGPASS invalid username-password pair or user is disabled."
	noAuth                   = "NOAUTH Authentication required."
)

func executeInSubscribeModeError(command string) string {
	return "ERR Can't execute '" +
		strings.ToLower(command) +
		"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
}
