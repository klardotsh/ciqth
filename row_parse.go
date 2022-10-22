package main

import (
	"errors"
	"fmt"
	"strconv"
	"time"
)

type ParsedRow struct {
	timestamp time.Time
	username  string
	direction rune
	sizeKB    uint64
}

func parseRow(row []string, currentLine int) (*ParsedRow, error) {
	if row[2] != "upload" && row[2] != "download" {
		return nil, errors.New(fmt.Sprintf("line %d: expected verb upload|download, got: %s", currentLine, row[2]))
	}

	parsedTime, err := time.Parse(time.UnixDate, row[0])
	if err != nil {
		return nil, err
	}

	parsedDirection, err := parseDirectionToChar(row[2])
	if err != nil {
		return nil, err
	}

	parsedSize, err := strconv.ParseUint(row[3], 10, 64)
	if err != nil {
		return nil, err
	}

	return &ParsedRow{
		timestamp: parsedTime,
		username:  row[1],
		direction: parsedDirection,
		sizeKB:    parsedSize,
	}, nil
}

// Go's type system doesn't allow any sort of narrowing here like more advanced
// languages (think TS, Rust, Haskell), so we instead defer sanity checking to
// runtime and have to return an ugly val+err tuple. Sigh.
//
// This is especially redundant in context: the CSV parser *also* runtime
// errors if invalid values are read, but of course this function doesn't know
// that, nor, necessarily, would someone trying to *use* this function.
func parseDirectionToChar(input string) (rune, error) {
	if input == "upload" {
		return 'U', nil
	}

	if input == "download" {
		return 'D', nil
	}

	// "nil" is not a valid rune, and to avoid string allocations here, the
	// return type is rune. Instead, return the null byte and rely on callers
	// checking the error value correctly. Our DB schema will at least scream
	// loudly about this if the null byte is attempted to be inserted.
	return 0, errors.New("invalid direction supplied, must be upload or download")
}
