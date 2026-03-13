package database

import (
	"fmt"
	"strconv"
	"strings"
)

func parseCommand(buf []byte) ([]string, error) {
	if len(buf) == 0 || buf[0] != '*' {
		return nil, fmt.Errorf("invalid command format")
	}

	offset := 1
	// get number of arguments
	numArgs := 0
	for i := 1; buf[i] != '\r'; i++ {
		numArgs = numArgs*10 + int(buf[i]-'0')
		offset++
	}
	offset += 2 // skip \r\n

	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		if buf[offset] != '$' {
			return nil, fmt.Errorf("invalid argument format")
		}
		offset++

		argLen := 0
		for j := offset; buf[j] != '\r'; j++ {
			argLen = argLen*10 + int(buf[j]-'0')
			offset++
		}
		offset += 2 // skip \r\n

		args[i] = string(buf[offset : offset+argLen])
		offset += argLen + 2 // skip argument and \r\n
	}
	return args, nil
}

func simpleString(s string) []byte {
	return []byte("+" + s + "\r\n")
}

func bulkString(s string, exist bool) []byte {
	if !exist {
		return []byte("$-1\r\n") // null bulk string
	}
	return []byte("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
}

func respInteger(i int) []byte {
	return []byte(":" + strconv.Itoa(i) + "\r\n")
}

func respArray(arr []string) []byte {
	if arr == nil {
		return []byte("*-1\r\n") // null array
	}
	var response strings.Builder
	response.WriteString("*" + strconv.Itoa(len(arr)) + "\r\n")
	for _, s := range arr {
		response.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
	}
	return []byte(response.String())
}
