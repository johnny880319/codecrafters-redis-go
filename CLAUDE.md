# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) and other coding agents when working with this repository.

## Project Overview

CodeCrafters "Build Your Own Redis" challenge: a minimal Redis-compatible server in Go with zero runtime dependencies.

## Build & Run Commands

```bash
# Build and run locally
./your_program.sh

# Build only (what CodeCrafters uses)
go build -o /tmp/codecrafters-build-redis-go cmd/my_redis/*.go

# Lint
golangci-lint run --fix

# Run tests
go test -short ./...

# Submit to CodeCrafters
git push origin master
```

## Architecture

**Entry point**: `cmd/my_redis/main.go` starts a TCP listener on `127.0.0.1:6379` and spawns one goroutine per connection.

**Core package**: `internal/database/` contains the Redis protocol parsing, command dispatch, in-memory storage, and RESP response helpers.

### Connection Flow

`Database.RunConnection()` wraps each connection in a `bufio.Reader`, repeatedly reads one full RESP command with `readCommand()`, dispatches it through `executeCommand()`, and writes the encoded response back to the client.

The request parser intentionally supports the command shape used by Redis clients: RESP arrays of bulk strings, such as `["SET", "key", "value"]`.

### Command Handler Pattern

Commands are dispatched in `Database.executeCommand()` (`internal/database/repl.go`) with a `switch` on the lowercase command name. Each command handler is a method on `*Database` and returns a RESP-encoded `[]byte`.

To add a new command:
1. Create a handler method `func (db *Database) cmdFoo(args []string) []byte` in the appropriate `cmd_*.go` file.
2. Register the command name in `executeCommand()` in `repl.go`.
3. Prefer the shared response helpers in `convert.go` instead of hand-building RESP bytes.

Command files are organized by category:
- `cmd_basic.go`: `PING`, `ECHO`, `SET`, `GET`, `TYPE`
- `cmd_list.go`: `RPUSH`, `LPUSH`, `LPOP`, `BLPOP`, `LRANGE`, `LLEN`
- `cmd_stream.go`: `XADD`, `XRANGE`, `XREAD`, blocking `XREAD`

### RESP Protocol

`convert.go` handles RESP request parsing and response serialization.

Important helpers:
- `readCommand()` parses one RESP array command from a buffered reader.
- `readRespLine()` reads CRLF-terminated RESP header lines.
- `simpleString()`, `bulkString()`, `respInteger()`, `respArray()`, and `simpleError()` build RESP responses.

`respArray()` accepts already encoded RESP elements (`[][]byte`) so nested arrays can be built by composing helper results.

### Data Storage

`Database` stores all data in an in-memory `map[string]dbEntry`. Each `dbEntry` has:
- `value any`: the stored value
- `vType ValueType`: `StringType`, `ListType`, or `StreamType`
- `expiresAt time.Time`: optional expiration timestamp

Expiration is checked lazily through `getEntry()`, which deletes expired keys before returning. Typed access helpers (`getStringEntry()`, `getListEntry()`, `getStreamEntry()`) centralize wrong-type handling.

### Streams

Streams are represented as `[]map[string]string`, where each entry stores its Redis stream ID under the `id` key and the remaining field/value pairs as map entries.

`cmd_stream.go` handles:
- ID validation and auto-generation for `*` and `<milliseconds>-*`
- Range reads with `XRANGE`
- Non-blocking and blocking reads with `XREAD`

### Concurrency

- `sync.RWMutex` protects all database state.
- `BLPOP` and blocking `XREAD` use channel-based waiters stored in `db.waiters`.
- `RPUSH`/`LPUSH` wake list waiters by sending popped values.
- `XADD` wakes stream waiters by notifying blocked readers to retry `XREAD`.

## Collaboration Style

This is a practice repository. For learning-oriented questions, explain the issue and tradeoffs first. Only edit code when the user explicitly asks for an implementation or patch.

## Code Style

- Prefer small, focused changes that match the existing command-handler style.
- Keep RESP formatting in helper functions.
- Keep command behavior simple and CodeCrafters-focused before broadening toward full Redis compatibility.
