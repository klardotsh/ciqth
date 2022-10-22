package main

import (
	"testing"
	"time"
)

func TestParseRow(t *testing.T) {
	input := make([]string, 4)
	input[0] = "Sun Apr 12 22:10:38 UTC 2020"
	input[1] = "sarah94"
	input[2] = "download"
	input[3] = "34"

	parsed, err := parseRow(input, 1)
	if err != nil {
		t.Fatal(err)
	}

	if parsed.timestamp.Year() != 2020 || parsed.timestamp.Month() != time.April {
		t.Fatal("incorrect date parsed from input")
	}

	if parsed.username != "sarah94" {
		t.Fatal("incorrect username parsed from input")
	}

	if parsed.direction != 'D' {
		t.Fatal("incorrect direction parsed from input")
	}

	if parsed.sizeKB != 34 {
		t.Fatal("incorrect size parsed from input")
	}
}
