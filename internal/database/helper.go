package database

import "time"

func (db *Database) getEntry(key string) (dbEntry, bool) {
	val, ok := db.data[key]
	if !ok {
		return dbEntry{}, false
	}
	// check if the key has expired
	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		delete(db.data, key)
		return dbEntry{}, false
	}
	return val, true
}
