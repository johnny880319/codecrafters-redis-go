package database

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func (db *Database) initializeAppendOnlyFile(config DBConfig) (err error) {
	if config.Appendonly != "yes" {
		return nil
	}

	appendOnlyPath, manifestPath, err := getAppendOnlyPaths(config)
	if err != nil {
		return fmt.Errorf("error getting appendonly file paths: %w", err)
	}

	err = createAOFIfNotExists(config, appendOnlyPath, manifestPath)
	if err != nil {
		return fmt.Errorf("error creating appendonly file: %w", err)
	}

	//nolint:gosec // This is redis behavior, we can assume the filename is safe
	aofFile, err := os.OpenFile(appendOnlyPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("error opening appendonly file: %w", err)
	}
	db.aofFile = aofFile

	err = db.replayAOFFile(appendOnlyPath)
	if err != nil {
		return fmt.Errorf("error replaying appendonly file: %w", err)
	}
	return nil
}

func getAppendOnlyPaths(config DBConfig) (string, string, error) {
	appendOnlyDir := filepath.Join(config.Dir, config.Appenddirname)
	if _, err := os.Stat(appendOnlyDir); os.IsNotExist(err) {
		err = os.MkdirAll(appendOnlyDir, 0o750)
		if err != nil {
			return "", "", fmt.Errorf("error creating appendonly directory: %w", err)
		}
	}

	appendOnlyPath := filepath.Join(appendOnlyDir, config.Appendfilename+".1.incr.aof")
	manifestPath := filepath.Join(appendOnlyDir, config.Appendfilename+".manifest")
	// if exists, extract the appendonly file name.
	if _, err := os.Stat(manifestPath); err == nil {
		//nolint:gosec // This is redis behavior, we can assume the filename is safe
		content, err := os.ReadFile(manifestPath)
		if err != nil {
			return "", "", fmt.Errorf("error reading appendonly manifest file: %w", err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			// file <filename> seq <number> type <type>
			parts := strings.Fields(line)
			if len(parts) != 6 || parts[0] != "file" || parts[2] != "seq" || parts[4] != "type" {
				continue
			}
			if parts[5] != "i" {
				continue
			}
			appendOnlyPath = filepath.Join(appendOnlyDir, parts[1])
			break
		}
	}
	return appendOnlyPath, manifestPath, nil
}

func createAOFIfNotExists(config DBConfig, appendOnlyPath string, manifestPath string) error {
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		//nolint:gosec // This is redis behavior, we can assume the filename is safe
		file, err := os.Create(manifestPath)
		if err != nil {
			return fmt.Errorf("error creating appendonly manifest file: %w", err)
		}
		description := fmt.Sprintf("file %s seq 1 type i", config.Appendfilename+".1.incr.aof")
		_, err = file.WriteString(description)
		if err != nil {
			return fmt.Errorf("error writing to appendonly manifest file: %w", err)
		}
		err = file.Close()
		if err != nil {
			return fmt.Errorf("error closing appendonly manifest file: %w", err)
		}
	}

	if _, err := os.Stat(appendOnlyPath); os.IsNotExist(err) {
		//nolint:gosec // This is redis behavior, we can assume the filename is safe
		file, err := os.Create(appendOnlyPath)
		if err != nil {
			return fmt.Errorf("error creating appendonly file: %w", err)
		}
		err = file.Close()
		if err != nil {
			return fmt.Errorf("error closing appendonly file: %w", err)
		}
	}
	return nil
}

func (db *Database) replayAOFFile(appendOnlyPath string) (err error) {
	//nolint:gosec // This is redis behavior, we can assume the filename is safe
	replayFile, err := os.Open(appendOnlyPath)
	if err != nil {
		return fmt.Errorf("error opening appendonly file for replay: %w", err)
	}
	defer func() {
		replayErr := replayFile.Close()
		err = errors.Join(err, replayErr)
	}()

	reader := bufio.NewReader(replayFile)
	virtualClient := &client{db: db, conn: nil, watched: make(map[string]string)}
	for {
		command, _, err := readCommand(reader)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading appendonly file: %w", err)
		}

		if len(command) == 0 {
			continue
		}

		_ = virtualClient.handleCommand(command)
	}
	return nil
}
