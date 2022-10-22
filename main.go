package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// In some future iteration of this, a proper migrations manager is in order,
// even if that's just something ruthlessly simple like
// https://github.com/mbucc/shmig
const createDBTables string = `
	CREATE TABLE IF NOT EXISTS transfers (
	id INTEGER NOT NULL PRIMARY KEY,

	-- There's not a particularly great way to index DATETIMEs in SQLite. For
	-- answering the "which events happened on X" day questions, this will for now
	-- trigger a full table scan, which is perhaps fine with the current data set,
	-- but fails in fairly obvious, linear ways as the set grows. See: musings in
	-- README.md about the compromises of how this data is stored.
	timestamp DATETIME NOT NULL,

	-- In something like PostgreSQL we might use a more efficient enum type.
	-- Given more time and a wish to optimize this SQLite implementation,
	-- we could make a joined table of (integer, text) to somewhat emulate
	-- enums while retaining their human readable formats. For now, this is
	-- a compromise that reduces disk usage (vs storing the full
	-- "upload"/"download" repeatedly), while not relying on app code to
	-- do enum conversions (which, IMO, violates the principle of there being
	-- one arbiter of truth in a relational DB based system: the database)
	direction TEXT CHECK(direction in ('U', 'D')) NOT NULL,

	username TEXT NOT NULL,

	size_kb INTEGER NOT NULL,

	-- In general this should make insertions dupe-safe: assume a user can only
	-- ever do one event at a given instant in time. This will likely be the
	-- biggest index in the entire DB, and may also have perf concerns due to the
	-- lack of available indexing on DATETIME columns as alluded to above.
	-- Brainstorming a bit, while I'm unsure how to do it in SQLite, in PostgreSQL
	-- land I might look to do a CTE-based insertion here that would hash the
	-- username and append that hash's bits to the DATETIME struct, and store that
	-- in a custom indexing table of some variety, which is the ultimate arbiter
	-- of uniqueness. More thought needed here; not solving problems I don't have
	-- yet.
	UNIQUE(username, timestamp)
	);

	CREATE INDEX IF NOT EXISTS transfers_by_username ON transfers(username);
	CREATE INDEX IF NOT EXISTS transfers_by_direction ON transfers(direction);
`

const insertTransferRow string = `
	INSERT INTO transfers (timestamp, direction, username, size_kb) VALUES (?, CHAR(?), ?, ?);
`

func main() {
	var skipDupes = flag.Bool("skipdupes", false, "skip duplicate rows rather than erroring on them")
	var analyze = flag.Bool("analyze", false, "analyze dataset after import (answer the example stakeholder questions)")
	flag.Parse()

	// In a production scenario we wouldn't be using sqlite here, but in
	// general, "use sqlite locally and postgres in prod" is somewhere between
	// a myth and a footgun, so this isn't abstracted out to a helper function
	// or anything of the sort quite yet. With something like Postgres,
	// however, we may want connection pooling or other helper logistics that
	// could serve to be isolated out from main() here
	db, err := sql.Open("sqlite3", "log_parser.db")
	if err != nil {
		log.Fatal("unable to init database")
	}
	if _, err := db.Exec(createDBTables); err != nil {
		log.Fatal(fmt.Printf("unable to migrate database: %s", err))
	}

	// TODO: don't hard-code this path. I debated between going Unix-ier here
	// and taking input over stdin, this was instead chosen somewhat
	// arbitrarily
	const filename string = "server_log.csv"
	inputFile, err := os.Open(filename)
	if err != nil {
		log.Fatal("unable to open input file")
	}
	defer inputFile.Close()

	log.Printf("Importing events from %s\n", filename)
	parser := csv.NewReader(inputFile)
	parser.FieldsPerRecord = 4
	currentLine := 0

	// TODO: this loop should be its own method that takes the parser by
	// pointer (or perhaps decoupled over a Reader interface) and returns a
	// channel of events for insertion by another goroutine(s), since parsing
	// should almost always be faster than the IO of insertion and, in general,
	// we can tolerate optimistic inserts with the DB schema we have (duplicate
	// rows will error out, but we don't strictly need those errors to come
	// sequentially. other inserts can finish while we parse the new error)
	//
	// For now, however, we'll stick with this serial and synchronous approach.
	// It's a MVP that works at the existing data scale.
	for {
		currentLine += 1
		record, err := parser.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		// A thought comes to mind here: if two log files are concatenated (or,
		// say, this tool suddenly starts accepting data over stdin instead of
		// reading from disk), this breaks slightly. The "correct" evolution is
		// probably to separate out file reading and line streaming such that
		// "line parsing" lives in its own function that can be called from
		// either flow, and header stripping is handled a layer above. For now,
		// since it's a 1-2h takehome, we'll stick to the quick and dirty
		// solution.
		if currentLine == 1 && record[0] == "timestamp" {
			continue
		}

		parsedRow, err := parseRow(record, currentLine)
		if err != nil {
			log.Fatal(err)
		}

		if _, err := db.Exec(
			insertTransferRow,
			parsedRow.timestamp,
			parsedRow.direction,
			parsedRow.username,
			parsedRow.sizeKB,
		); err != nil {
			// There's probably much cleaner ways to do this, and if there's
			// not, I have some questions about the Golang SQL driver's
			// ergonomics
			if *skipDupes && strings.Contains(err.Error(), "UNIQUE constraint failed: transfers.username, transfers.time") {
				log.Printf("line %d: data already imported, skipping\n", currentLine)
				continue
			}

			log.Fatal(fmt.Printf("line %d: unable to insert row: %s", currentLine, err))
		}
	}

	log.Println("Success!")

	if *analyze {
		log.Println("Analyzing data...")

		analytics, err := runAnalyticsQuery(db)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("How many users accessed the server?: %d\n", analytics.numUniqueUsers)
		log.Printf("How many uploads were larger than 50kB?: %d\n", analytics.numLargeUploads)
		log.Printf("How many times did jeff22 upload to the server on April 15th, 2020?: %d\n", analytics.numJeffUploads)
	}
}
