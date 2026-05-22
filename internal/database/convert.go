package database

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func readResponse(reader *bufio.Reader) (string, error) {
	line, err := readRespLine(reader)
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '+' {
		return "", fmt.Errorf("invalid response format")
	}
	return line[1:], nil
}

func readCommand(reader *bufio.Reader) ([]string, string, error) {
	originalCommand := ""
	line, err := readRespLine(reader)
	if err != nil {
		return nil, "", err
	}
	originalCommand += line + "\r\n"

	if len(line) == 0 || line[0] != '*' {
		return nil, "", fmt.Errorf("invalid command format")
	}

	numArgs, err := strconv.Atoi(line[1:])
	if err != nil || numArgs < 0 {
		return nil, "", fmt.Errorf("invalid number of arguments: %s", line[1:])
	}

	args := make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		line, err := readRespLine(reader)
		if err != nil {
			return nil, "", err
		}
		originalCommand += line + "\r\n"

		if len(line) == 0 || line[0] != '$' {
			return nil, "", fmt.Errorf("invalid argument format")
		}

		argLen, err := strconv.Atoi(line[1:])
		if err != nil || argLen < 0 {
			return nil, "", fmt.Errorf("invalid argument length: %s", line[1:])
		}

		buf := make([]byte, argLen+2) // +2 for \r\n
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, "", err
		}
		if !strings.HasSuffix(string(buf), "\r\n") {
			return nil, "", fmt.Errorf("invalid argument format: missing CRLF")
		}

		args[i] = string(buf[:argLen])
		originalCommand += args[i] + "\r\n"
	}
	return args, originalCommand, nil
}

func readRespLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("invalid RESP line: %s", line)
	}
	return strings.TrimSuffix(line, "\r\n"), nil
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

func respArray(arr [][]byte) []byte {
	if arr == nil {
		return []byte("*-1\r\n") // null array
	}
	result := []byte("*" + strconv.Itoa(len(arr)) + "\r\n")
	for _, elem := range arr {
		result = append(result, elem...)
	}
	return result
}

func simpleError(msg string) []byte {
	return []byte("-ERR " + msg + "\r\n")
}
