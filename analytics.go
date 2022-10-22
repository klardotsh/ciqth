package main

import (
	"database/sql"
	"errors"
)

type AnalyticsResult struct {
	numUniqueUsers  uint
	numLargeUploads uint
	numJeffUploads  uint
}

func runAnalyticsQuery(db *sql.DB) (*AnalyticsResult, error) {
	// This function is jammed in here somewhat haphazardly for now, as it's
	// the most likely to change with time, and frankly, hard-coding these
	// queries in here is probably not the best UX anyway (I'd honestly rather
	// enable the folks asking these questions to just write their own SQL and
	// give them an RO replica of the DB to run whatever queries they want), so
	// it's not worth investing in a "more correct" solution quite yet until
	// there's further discussion.
	result, err := db.Query(`
		WITH large_uploads(count) AS ( SELECT COUNT(*) FROM transfers WHERE size_kb > 50 ),
		jeff_uploads(count) AS (
			SELECT COUNT(*) from transfers WHERE username = 'jeff22' AND DATE(timestamp) = '2020-04-15'
		)
		SELECT
			COUNT(DISTINCT transfers.username),
			large_uploads.count,
			jeff_uploads.count
		FROM transfers, large_uploads, jeff_uploads;
	`)

	if err != nil {
		return nil, err
	}
	defer result.Close()

	if !result.Next() {
		return nil, errors.New("no result rows returned")
	}

	ret := new(AnalyticsResult)

	if err := result.Scan(&ret.numUniqueUsers, &ret.numLargeUploads, &ret.numJeffUploads); err != nil {
		return nil, err
	}

	return ret, nil
}
