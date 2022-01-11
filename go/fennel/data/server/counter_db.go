package main

import (
	"database/sql"
	"fennel/data/lib"
	"fennel/db"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"time"
)

type CounterBucket struct {
	CounterType lib.CounterType `db:"counter_type"`
	Window      lib.Window      `db:"window_type"`
	Idx         uint64
	Key         string
	Count       uint64
}

const (
	// GRANULARITY to implement rolling windows, we partition any time window in this many static
	// sub-windows. At query time, we just look at this many most recent windows
	GRANULARITY = 100

	// FOREVER_BUCKET_INDEX for "forever" window, since there is no rolling counts,
	// we store a single bucket this constant is the index for that
	FOREVER_BUCKET_INDEX = 0
)

const (
	COUNTER_TABLE    = "counter_bucket"
	CHECKPOINT_TABLE = "checkpoint"
)

func createCounterTables() error {
	sql := fmt.Sprintf(`CREATE TABLE %s (
		"counter_type" integer NOT NULL,
		"window_type" integer NOT NULL,
		"idx" integer NOT NULL,
		"count" integer NOT NULL DEFAULT 0,
		"key" blob NOT NULL,
		PRIMARY KEY(counter_type, window_type, idx, key)
	  );`, COUNTER_TABLE)

	//log.Printf("Creating table '%s'...%s", COUNTER_TABLE, sql)
	statement, err := db.DB.Prepare(sql)
	if err != nil {
		return err
	}
	statement.Exec()
	log.Printf("'%s' table created\n", COUNTER_TABLE)

	// now create checkpoint table
	sql = fmt.Sprintf(`CREATE TABLE %s (
		"counter_type" INTEGER NOT NULL,
		"checkpoint" INTEGER NOT NULL DEFAULT 0,
		UNIQUE(counter_type)
	  );`, CHECKPOINT_TABLE)

	//log.Printf("Creating table '%s'...%s", CHECKPOINT_TABLE, sql)
	statement, err = db.DB.Prepare(sql)
	if err != nil {
		return err
	}
	statement.Exec()
	log.Printf("'%s' table created\n", CHECKPOINT_TABLE)
	return nil
}

func tsToIndex(ts lib.Timestamp, window lib.Window) (uint64, error) {
	switch window {
	case lib.Window_HOUR:
		return uint64(ts / (3600 / GRANULARITY)), nil
	case lib.Window_DAY:
		return uint64(ts / (3600 * 24 / GRANULARITY)), nil
	case lib.Window_WEEK:
		return uint64(ts / (3600 * 24 * 7 / GRANULARITY)), nil
	case lib.Window_MONTH:
		return uint64(ts / (3600 * 24 * 30 / GRANULARITY)), nil
	case lib.Window_QUARTER:
		return uint64(ts / (3600 * 24 * 90 / GRANULARITY)), nil
	case lib.Window_YEAR:
		return uint64(ts / (3600 * 24 * 365 / GRANULARITY)), nil
	case lib.Window_FOREVER:
		// for forever window, we have literally a single bucket
		return FOREVER_BUCKET_INDEX, nil
	default:
		return 0, fmt.Errorf("invalid window : %v", window)
	}
}

func keyToString(k lib.Key) string {
	return fmt.Sprintf("%d", k)
}

func counterIncrement(ct lib.CounterType, window lib.Window, key lib.Key, ts lib.Timestamp, count uint64) error {
	index, err := tsToIndex(ts, window)
	if err != nil {
		return err
	}
	bucket := CounterBucket{ct, window, index, keyToString(key), count}
	return counterDBIncrement(bucket)
}

func counterGet(request lib.GetCountRequest) (uint64, error) {
	if request.Timestamp == 0 {
		request.Timestamp = lib.Timestamp(time.Now().Unix())
	}
	index, err := tsToIndex(request.Timestamp, request.Window)
	if err != nil {
		return 0, err
	}
	bucket := CounterBucket{request.CounterType, request.Window, index, keyToString(request.Key), 0}
	return counterDBGet(bucket)
}

// updates the bucket identified by (bucket.counter_type, bucket.window, bucket.index, bucket.key)
// by incrementing its count by bucket.count
// TODO: make this batched and updaate at least all windows for a single event together
func counterDBIncrement(bucket CounterBucket) error {
	_, err := db.DB.NamedExec(fmt.Sprintf(`
		INSERT INTO %s 
			( counter_type, window_type, idx, key, count)
        VALUES 
			(:counter_type, :window_type, :idx, :key, :count)
		ON CONFLICT(counter_type, window_type, idx, key)
		DO UPDATE SET
			count = count + :count
		;`, COUNTER_TABLE),
		bucket)
	return err
}

// returns count of 'num' buckets such that:
//	bucket is identified by (bucket.counter_type, bucket.window, bucket.key)
// and bucket index is between (bucket.index - 100, bucket.index] left exclusive, right inclusive
// however, if window is forever, the index field doesn't matter (forever uses a single bucket)
// the 'GetCount' field of input bucket is ignored
func counterDBGet(bucket CounterBucket) (uint64, error) {
	query := fmt.Sprintf(`
		SELECT SUM(count) as total
		FROM %s	
		WHERE 
			counter_type = :counter_type
			AND window_type = :window_type
			AND key = :key 
		`, COUNTER_TABLE)
	if bucket.Window != lib.Window_FOREVER {
		query = fmt.Sprintf("%s AND idx > :idx - %d AND idx <= :idx;", query, GRANULARITY)
	} else {
		query = fmt.Sprintf("%s AND idx = %d", query, FOREVER_BUCKET_INDEX)
	}
	//log.Printf("Counter storage, get query: %s\n", query)
	statement, err := db.DB.PrepareNamed(query)
	if err != nil {
		return 0, err
	}
	row := statement.QueryRow(bucket)
	var total sql.NullInt64
	row.Scan(&total)
	if total.Valid {
		return uint64(total.Int64), nil
	} else {
		return 0, nil
	}
}

func counterDBPrintAll() error {
	// this is slow and will do full table scan. Just use it for debugging/dev
	var buckets []CounterBucket
	err := db.DB.Select(&buckets, fmt.Sprintf("SELECT * FROM %s", COUNTER_TABLE))
	if err != nil {
		return err
	}
	for _, item := range buckets {
		fmt.Printf("%#v\n", item)
	}
	return nil
}

func counterDBGetCheckpoint(ct lib.CounterType) (lib.OidType, error) {
	row := db.DB.QueryRow(fmt.Sprintf(`
		SELECT checkpoint
		FROM %s
		WHERE counter_type = ?;
	`, CHECKPOINT_TABLE), ct)
	var checkpoint uint64
	err := row.Scan(&checkpoint)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	} else if err == sql.ErrNoRows {
		// this happens when no matching row was found. By default, checkpoint is zero
		return 0, nil
	} else {
		return lib.OidType(checkpoint), nil
	}
}

func counterDBSetCheckpoint(ct lib.CounterType, checkpoint lib.OidType) error {
	_, err := db.DB.Exec(fmt.Sprintf(`
		INSERT INTO %s (counter_type, checkpoint)
        VALUES (?, ?)
		ON CONFLICT(counter_type)
		DO UPDATE SET
			checkpoint = ?
		;`, CHECKPOINT_TABLE), ct, checkpoint, checkpoint)
	return err
}
