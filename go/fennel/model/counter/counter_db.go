package counter

import (
	"database/sql"
	"fennel/instance"
	"fennel/lib/action"
	"fennel/lib/counter"
	"fmt"
	"time"
)

type bucket struct {
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

func Increment(this instance.Instance, ct counter.CounterType, window counter.Window, key counter.Key, ts action.Timestamp, count uint64) error {
	index, err := tsToIndex(ts, window)
	if err != nil {
		return err
	}
	bucket := bucket{ct, window, index, keyToString(key), count}
	return dbIncrement(this, bucket)
}

func Get(this instance.Instance, request counter.GetCountRequest) (uint64, error) {
	if request.Timestamp == 0 {
		request.Timestamp = action.Timestamp(time.Now().Unix())
	}
	index, err := tsToIndex(request.Timestamp, request.Window)
	if err != nil {
		return 0, err
	}
	bucket := bucket{request.CounterType, request.Window, index, keyToString(request.Key), 0}
	return dbGet(this, bucket)
}

// updates the bucket identified by (bucket.counter_type, bucket.window, bucket.index, bucket.key)
// by incrementing its count by bucket.count
// TODO: make this batched and updaate at least all windows for a single event together
func dbIncrement(this instance.Instance, bucket bucket) error {
	if len(bucket.Key) > 256 {
		return fmt.Errorf("too long key: keys can only be upto 256 chars")
	}
	_, err := this.DB.NamedExec(`
		INSERT INTO counter_bucket
			( counter_type, window_type, idx, zkey, count)
        VALUES 
			(:counter_type, :window_type, :idx, :key, :count)
		ON DUPLICATE KEY
		UPDATE
			count = count + :count
		;`, bucket)
	return err
}

// returns count of 'num' buckets such that:
//	bucket is identified by (bucket.counter_type, bucket.window, bucket.key)
// and bucket index is between (bucket.index - 100, bucket.index] left exclusive, right inclusive
// however, if window is forever, the index field doesn't matter (forever uses a single bucket)
// the 'GetCount' field of input bucket is ignored
func dbGet(this instance.Instance, bucket bucket) (uint64, error) {
	query := `
		SELECT SUM(count) as total
		FROM counter_bucket
		WHERE 
			counter_type = :counter_type
			AND window_type = :window_type
			AND zkey = :key 
		`
	if bucket.Window != counter.Window_FOREVER {
		query = fmt.Sprintf("%s AND idx > :idx - %d AND idx <= :idx;", query, GRANULARITY)
	} else {
		query = fmt.Sprintf("%s AND idx = %d", query, FOREVER_BUCKET_INDEX)
	}
	//log.Printf("Counter storage, get query: %s\n", query)
	statement, err := this.DB.PrepareNamed(query)
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
