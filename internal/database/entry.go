// Entry helpers may delete expired keys while reading, so callers must hold db.rwMu.Lock.
// Do not call these helpers under RLock unless expiration deletion is moved elsewhere.

package database

import (
	"fmt"
	"time"
)

// getEntry returns the entry for key and lazily removes it if expired.
// Caller must hold db.rwMu.Lock because this may mutate db.data.
func (db *Database) getEntry(key string) (dbEntry, bool) {
	val, ok := db.data[key]
	if !ok {
		return dbEntry{}, false
	}
	// check if the key has expired
	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		delete(db.data, key)
		db.versions[key]++
		return dbEntry{}, false
	}
	return val, true
}

// getStringEntry follows getEntry's locking contract.
func (db *Database) getStringEntry(key string) (string, dbEntry, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return "", dbEntry{}, false, nil
	}
	if entry.vType != StringType {
		return "", dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
	}
	if content, ok := entry.value.(string); ok {
		return content, entry, true, nil
	}
	return "", dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
}

// getListEntry follows getEntry's locking contract.
func (db *Database) getListEntry(key string) ([]string, dbEntry, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return nil, dbEntry{}, false, nil
	}
	if entry.vType != ListType {
		return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
	}
	if content, ok := entry.value.([]string); ok {
		return content, entry, true, nil
	}
	return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
}

// getStreamEntry follows getEntry's locking contract.
func (db *Database) getStreamEntry(key string) ([]map[string]string, dbEntry, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return nil, dbEntry{}, false, nil
	}
	if entry.vType != StreamType {
		return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
	}
	if content, ok := entry.value.([]map[string]string); ok {
		return content, entry, true, nil
	}
	return nil, dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
}

// getSortedSetEntry follows getEntry's locking contract.
func (db *Database) getSortedSetEntry(key string) (map[string]float64, dbEntry, bool, error) {
	entry, exists := db.getEntry(key)
	if !exists {
		return make(map[string]float64), dbEntry{}, false, nil
	}
	if entry.vType != SortedSetType {
		return make(map[string]float64), dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
	}
	if content, ok := entry.value.(map[string]float64); ok {
		return content, entry, true, nil
	}
	return make(map[string]float64), dbEntry{}, false, fmt.Errorf("wrong type of value for key '%s'", key)
}

func (db *Database) setStringEntry(key string, value string, expiresAt time.Time) {
	db.data[key] = dbEntry{
		value:     value,
		vType:     StringType,
		expiresAt: expiresAt,
	}
	db.versions[key]++
}

func (db *Database) setListEntry(key string, value []string, expiresAt time.Time) {
	db.data[key] = dbEntry{
		value:     value,
		vType:     ListType,
		expiresAt: expiresAt,
	}
	db.versions[key]++
}

func (db *Database) setStreamEntry(key string, value []map[string]string, expiresAt time.Time) {
	db.data[key] = dbEntry{
		value:     value,
		vType:     StreamType,
		expiresAt: expiresAt,
	}
	db.versions[key]++
}

func (db *Database) setSortedSetEntry(key string, value map[string]float64, expiresAt time.Time) {
	db.data[key] = dbEntry{
		value:     value,
		vType:     SortedSetType,
		expiresAt: expiresAt,
	}
	db.versions[key]++
}
