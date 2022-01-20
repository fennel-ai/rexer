package main

import (
	"database/sql"
	"fennel/db"
	"fennel/lib/action"
	"fennel/lib/counter"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"time"
)

type CounterBucket struct {
	CounterType counter.CounterType `db:"counter_type"`
	Window      counter.Window      `db:"window_type"`
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
	COUNTER_TABLE = "counter_bucket"
)

type CounterTable struct {
	db.Table
}

func NewCounterTable(conn db.Connection) (CounterTable, error) {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
		counter_type integer NOT NULL,
		window_type integer NOT NULL,
		idx integer NOT NULL,
		count integer NOT NULL DEFAULT 0,
		zkey varchar(256) NOT NULL,
		PRIMARY KEY(counter_type, window_type, idx, zkey)
	  );`, COUNTER_TABLE)
	conf := db.TableConfig{SQL: sql, Name: COUNTER_TABLE, DB: conn, DropTable: true}
	resource, err := conf.Materialize()
	if err != nil {
		return CounterTable{}, err
	}
	return CounterTable{resource.(db.Table)}, err
}

func tsToIndex(ts action.Timestamp, window counter.Window) (uint64, error) {
	switch window {
	case counter.Window_HOUR:
		return uint64(ts / (3600 / GRANULARITY)), nil
	case counter.Window_DAY:
		return uint64(ts / (3600 * 24 / GRANULARITY)), nil
	case counter.Window_WEEK:
		return uint64(ts / (3600 * 24 * 7 / GRANULARITY)), nil
	case counter.Window_MONTH:
		return uint64(ts / (3600 * 24 * 30 / GRANULARITY)), nil
	case counter.Window_QUARTER:
		return uint64(ts / (3600 * 24 * 90 / GRANULARITY)), nil
	case counter.Window_YEAR:
		return uint64(ts / (3600 * 24 * 365 / GRANULARITY)), nil
	case counter.Window_FOREVER:
		// for forever window, we have literally a single bucket
		return FOREVER_BUCKET_INDEX, nil
	default:
		return 0, fmt.Errorf("invalid window : %v", window)
	}
}

func keyToString(k counter.Key) string {
	return fmt.Sprintf("%d", k)
}

func (table CounterTable) counterIncrement(ct counter.CounterType, window counter.Window, key counter.Key, ts action.Timestamp, count uint64) error {
	index, err := tsToIndex(ts, window)
	if err != nil {
		return err
	}
	bucket := CounterBucket{ct, window, index, keyToString(key), count}
	return table.counterDBIncrement(bucket)
}

func (table CounterTable) counterGet(request counter.GetCountRequest) (uint64, error) {
	if request.Timestamp == 0 {
		request.Timestamp = action.Timestamp(time.Now().Unix())
	}
	index, err := tsToIndex(request.Timestamp, request.Window)
	if err != nil {
		return 0, err
	}
	bucket := CounterBucket{request.CounterType, request.Window, index, keyToString(request.Key), 0}
	return table.counterDBGet(bucket)
}

// updates the bucket identified by (bucket.counter_type, bucket.window, bucket.index, bucket.key)
// by incrementing its count by bucket.count
// TODO: make this batched and updaate at least all windows for a single event together
func (table CounterTable) counterDBIncrement(bucket CounterBucket) error {
	if len(bucket.Key) > 256 {
		return fmt.Errorf("too long key: keys can only be upto 256 chars")
	}
	_, err := table.DB.NamedExec(fmt.Sprintf(`
		INSERT INTO %s 
			( counter_type, window_type, idx, zkey, count)
        VALUES 
			(:counter_type, :window_type, :idx, :key, :count)
		ON DUPLICATE KEY
		UPDATE
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
func (table CounterTable) counterDBGet(bucket CounterBucket) (uint64, error) {
	query := fmt.Sprintf(`
		SELECT SUM(count) as total
		FROM %s	
		WHERE 
			counter_type = :counter_type
			AND window_type = :window_type
			AND zkey = :key 
		`, COUNTER_TABLE)
	if bucket.Window != counter.Window_FOREVER {
		query = fmt.Sprintf("%s AND idx > :idx - %d AND idx <= :idx;", query, GRANULARITY)
	} else {
		query = fmt.Sprintf("%s AND idx = %d", query, FOREVER_BUCKET_INDEX)
	}
	//log.Printf("Counter storage, get query: %s\n", query)
	statement, err := table.DB.PrepareNamed(query)
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
