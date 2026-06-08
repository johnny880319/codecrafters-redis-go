package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func (db *Database) readRDBFile(config DBConfig) error {
	if config.Dir == "" || config.DBFilename == "" {
		return nil
	}
	if _, err := os.Stat(filepath.Join(config.Dir, config.DBFilename)); os.IsNotExist(err) {
		return nil
	}

	data, err := os.ReadFile(filepath.Join(config.Dir, config.DBFilename))
	if err != nil {
		return fmt.Errorf("error reading RDB file: %w", err)
	}

	db.data, err = parseRDBBytes(data)
	if err != nil {
		return fmt.Errorf("error parsing RDB file: %w", err)
	}
	return nil
}

func parseRDBBytes(data []byte) (map[string]dbEntry, error) {
	offset := 0
	entries := make(map[string]dbEntry)

	offset, err := parseRDBHeader(data, offset)
	if err != nil {
		return nil, err
	}

	offset, err = parseRDBMetadata(data, offset)
	if err != nil {
		return nil, err
	}

	for offset < len(data) && data[offset] != 0xFF {
		parsedEntries, newOffset, err := parseRDBEntries(data, offset)
		if err != nil {
			return nil, fmt.Errorf("error parsing RDB entry: %w", err)
		}
		offset = newOffset
		if len(entries) == 0 {
			entries = parsedEntries
		}
	}

	_, err = parseRDBEnd(data, offset)
	if err != nil {
		return nil, fmt.Errorf("error parsing RDB end: %w", err)
	}
	return entries, nil
}

func parseRDBHeader(data []byte, offset int) (int, error) {
	if offset != 0 || len(data) < 9 || string(data[:5]) != "REDIS" {
		return 0, fmt.Errorf("invalid RDB header")
	}
	return 9, nil
}

func parseRDBMetadata(data []byte, offset int) (int, error) {
	for offset < len(data) && data[offset] == 0xFA {
		// metadata name
		_, newOffset, err := parseRDBString(data, offset+1)
		if err != nil {
			return 0, fmt.Errorf("error parsing RDB metadata name: %w", err)
		}
		// metadata value
		_, newOffset, err = parseRDBString(data, newOffset)
		if err != nil {
			return 0, fmt.Errorf("error parsing RDB metadata value: %w", err)
		}
		offset = newOffset
	}
	return offset, nil
}

func parseRDBEntries(data []byte, offset int) (map[string]dbEntry, int, error) {
	dbEntries := make(map[string]dbEntry)
	if len(data) <= offset || data[offset] != 0xFE {
		return nil, 0, fmt.Errorf("invalid RDB entry header")
	}
	offset++
	// skip database index
	_, offset, err := parseRDBLength(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("error parsing RDB database index: %w", err)
	}

	if len(data) <= offset || data[offset] != 0xFB {
		return nil, 0, fmt.Errorf("invalid RDB entry header")
	}
	offset++
	hashTableSize, offset, err := parseRDBLength(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("error parsing RDB hash table size: %w", err)
	}
	// skip expiry count
	_, offset, err = parseRDBLength(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("error parsing RDB expiry count: %w", err)
	}

	for i := 0; i < hashTableSize; i++ {
		key, entry, newOffset, err := parseRDBEntry(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("error parsing RDB entry: %w", err)
		}
		dbEntries[key] = entry
		offset = newOffset
	}
	return dbEntries, offset, nil
}

func parseRDBEnd(data []byte, offset int) (int, error) {
	if len(data) != offset+9 || data[offset] != 0xFF {
		return 0, fmt.Errorf("invalid RDB end header")
	}
	return offset + 9, nil
}

func parseRDBLength(data []byte, offset int) (int, int, error) {
	if len(data) <= offset {
		return 0, 0, fmt.Errorf("unexpected end of RDB data")
	}

	switch data[offset] >> 6 {
	case 0:
		return int(data[offset] & 0x3F), offset + 1, nil
	case 1:
		if len(data) <= offset+1 {
			return 0, 0, fmt.Errorf("unexpected end of RDB data")
		}
		return int(data[offset]&0x3F)<<8 | int(data[offset+1]), offset + 2, nil
	case 2:
		if len(data) <= offset+4 {
			return 0, 0, fmt.Errorf("unexpected end of RDB data")
		}
		return int(data[offset+1])<<24 | int(data[offset+2])<<16 | int(data[offset+3])<<8 | int(data[offset+4]),
			offset + 5, nil
	default:
		return 0, 0, fmt.Errorf("unsupported RDB length encoding")
	}
}

func parseRDBString(data []byte, offset int) (string, int, error) {
	if len(data) <= offset {
		return "", 0, fmt.Errorf("unexpected end of RDB data while reading string length")
	}
	switch data[offset] {
	case 0xC0:
		if len(data) <= offset+1 {
			return "", 0, fmt.Errorf("unexpected end of RDB data")
		}
		//nolint:gosec // This is redis behavior, we can assume the value will not overflow
		return strconv.Itoa(int(int8(data[offset+1]))), offset + 2, nil
	case 0xC1:
		if len(data) <= offset+2 {
			return "", 0, fmt.Errorf("unexpected end of RDB data")
		}
		// little endian
		return strconv.Itoa(int(int16(data[offset+2])<<8 | int16(data[offset+1]))), offset + 3, nil
	case 0xC2:
		if len(data) <= offset+4 {
			return "", 0, fmt.Errorf("unexpected end of RDB data")
		}
		// little endian
		return strconv.Itoa(int(
			int32(data[offset+4])<<24 |
				int32(data[offset+3])<<16 |
				int32(data[offset+2])<<8 |
				int32(data[offset+1]),
		)), offset + 5, nil
	}

	length, newOffset, err := parseRDBLength(data, offset)
	if err != nil {
		return "", 0, fmt.Errorf("error parsing RDB string length: %w", err)
	}
	if len(data) < newOffset+length {
		return "", 0, fmt.Errorf("unexpected end of RDB data while reading string")
	}
	return string(data[newOffset : newOffset+length]), newOffset + length, nil
}

func parseRDBEntry(data []byte, offset int) (string, dbEntry, int, error) {
	switch data[offset] {
	case 0x00:
		key, newOffset, err := parseRDBString(data, offset+1)
		if err != nil {
			return "", dbEntry{}, 0, fmt.Errorf("error parsing RDB string key: %w", err)
		}
		value, newOffset, err := parseRDBString(data, newOffset)
		if err != nil {
			return "", dbEntry{}, 0, fmt.Errorf("error parsing RDB string value: %w", err)
		}
		return key, dbEntry{vType: StringType, value: value}, newOffset, nil
	case 0xFC:
		// The expire timestamp, expressed in Unix time,
		// stored as an 8-byte unsigned long, in little-endian (read right-to-left).
		expireMs := int64(data[offset+1]) | int64(data[offset+2])<<8 | int64(data[offset+3])<<16 | int64(data[offset+4])<<24 |
			int64(data[offset+5])<<32 | int64(data[offset+6])<<40 | int64(data[offset+7])<<48 | int64(data[offset+8])<<56
		key, entry, newOffset, err := parseRDBEntry(data, offset+9)
		if err != nil {
			return "", dbEntry{}, 0, fmt.Errorf("error parsing RDB entry with expiry: %w", err)
		}
		entry.expiresAt = time.UnixMilli(expireMs)
		return key, entry, newOffset, nil
	case 0xFD:
		// The expire timestamp, expressed in Unix time,
		// stored as an 4-byte unsigned integer, in little-endian (read right-to-left).
		expireSec := int64(data[offset+1]) | int64(data[offset+2])<<8 | int64(data[offset+3])<<16 | int64(data[offset+4])<<24
		key, entry, newOffset, err := parseRDBEntry(data, offset+5)
		if err != nil {
			return "", dbEntry{}, 0, fmt.Errorf("error parsing RDB entry with expiry: %w", err)
		}
		entry.expiresAt = time.Unix(expireSec, 0)
		return key, entry, newOffset, nil
	default:
		return "", dbEntry{}, 0, fmt.Errorf("unsupported RDB entry type: %x", data[offset])
	}
}
