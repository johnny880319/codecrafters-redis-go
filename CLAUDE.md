# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CodeCrafters "Build Your Own Redis" challenge — a minimal Redis-compatible server in Go with zero external dependencies.

## Build & Run Commands

```bash
# Build and run locally
./your_program.sh

# Build only (what CodeCrafters uses)
go build -o /tmp/codecrafters-build-redis-go cmd/my_redis/*.go

# Lint (runs automatically on pre-commit via lefthook)
golangci-lint run --fix

# Run tests (runs automatically on pre-push via lefthook)
go test -short ./...

# Submit to CodeCrafters
git push origin master
```

## Architecture

**Entry point**: `cmd/my_redis/main.go` — TCP listener on `127.0.0.1:6379`, spawns a goroutine per connection.

**Core package**: `internal/database/` — all Redis logic lives here.

### Command Handler Pattern

Commands are dispatched via a function map in `Database.getCommandMap()` (`internal/database/repl.go`). Each command is a method on `*Database` returning `[]byte` (RESP-encoded response).

To add a new command:
1. Create a handler method `func (db *Database) cmdFoo(args []string) []byte` in the appropriate `cmd_*.go` file
2. Register it in `getCommandMap()` in `repl.go`

Command files are organized by category:
- `cmd_basic.go` — PING, ECHO, SET, GET
- `cmd_list.go` — RPUSH, LPUSH, LPOP, BLPOP, LRANGE, LLEN
- `cmd_stream.go` — TYPE

### RESP Protocol

`convert.go` handles all RESP serialization/deserialization. Key functions: `parseCommand()` for parsing incoming RESP arrays, and `simpleString()`, `bulkString()`, `respInteger()`, `respArray()` for building responses.

### Data Storage

Single `Database` struct with an in-memory `map[string]dbEntry`. Each `dbEntry` holds a polymorphic `any` value (string or []string), a `ValueType` discriminator, and an optional expiration time. Expiration is checked lazily on GET.

### Concurrency

- `sync.RWMutex` protects all database state
- BLPOP uses a channel-based waiter pattern: blocked clients register a `chan string` in `db.waiters`, and RPUSH/LPUSH wake them by sending values

## Code Style

- Formatter: gofumpt (stricter than gofmt)
- Linter: golangci-lint with 21+ linters enabled (see `.golangci.yml`)
- Line length limit: 120 characters
- Cognitive complexity threshold: 20
