package database

const (
	xaddIDNotGreaterThanZero = "The ID specified in XADD must be greater than 0-0"
	xaddIDNotGreaterThanLast = "The ID specified in XADD is equal or smaller than the target stream top item"
	incrValueNotInteger      = "value is not an integer or out of range"
	execWithoutMulti         = "EXEC without MULTI"
	discardWithoutMulti      = "DISCARD without MULTI"
)
