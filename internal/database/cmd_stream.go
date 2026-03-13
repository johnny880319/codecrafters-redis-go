package database

func (db *Database) cmdType(args []string) []byte {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) != 1 {
		return []byte("-ERR wrong number of arguments for 'TYPE' command\r\n")
	}
	key := args[0]

	entry, exists := db.data[key]
	if !exists {
		return simpleString("none")
	}

	switch entry.vType {
	case StringType:
		return simpleString("string")
	case ListType:
		return simpleString("list")
	default:
		return simpleString("unknown")
	}
}
