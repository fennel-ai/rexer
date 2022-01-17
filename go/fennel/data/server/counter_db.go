package main

import (
	"database/sql"
	"fennel/data/lib"
	"fennel/db"
	profileLib "fennel/profile/lib"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
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

type CounterTable struct {
	db.Table
}

type CheckpointTable struct {
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

func NewCheckpointTable(conn db.Connection) (CheckpointTable, error) {
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS  %s(
		counter_type INTEGER NOT NULL,
		checkpoint INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(counter_type)
	  );`, CHECKPOINT_TABLE)
	conf := db.TableConfig{SQL: sql, Name: CHECKPOINT_TABLE, DB: conn, DropTable: true}
	resource, err := conf.Materialize()
	if err != nil {
		return CheckpointTable{}, err
	}
	return CheckpointTable{resource.(db.Table)}, err
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

func (table CounterTable) counterIncrement(ct lib.CounterType, window lib.Window, key lib.Key, ts lib.Timestamp, count uint64) error {
	index, err := tsToIndex(ts, window)
	if err != nil {
		return err
	}
	bucket := CounterBucket{ct, window, index, keyToString(key), count}
	return table.counterDBIncrement(bucket)
}

func (table CounterTable) counterGet(request lib.GetCountRequest) (uint64, error) {
	if request.Timestamp == 0 {
		request.Timestamp = lib.Timestamp(time.Now().Unix())
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
	if bucket.Window != lib.Window_FOREVER {
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

func (table CheckpointTable) counterDBGetCheckpoint(ct lib.CounterType) (profileLib.OidType, error) {
	row := table.DB.QueryRow(fmt.Sprintf(`
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
		return profileLib.OidType(checkpoint), nil
	}
}

func (table CheckpointTable) counterDBSetCheckpoint(ct lib.CounterType, checkpoint profileLib.OidType) error {
	_, err := table.DB.Exec(fmt.Sprintf(`
		INSERT INTO %s (counter_type, checkpoint)
        VALUES (?, ?)
		ON DUPLICATE KEY
		UPDATE
			checkpoint = ?
		;`, CHECKPOINT_TABLE), ct, checkpoint, checkpoint)
	return err
}
