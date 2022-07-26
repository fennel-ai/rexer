package db

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Create prometheus collector of badger expvar stats.
// Note: the exported names should match the names in the badger expvar stats
// defined at https://github.com/dgraph-io/badger/blob/master/y/metrics.go.
var badgerExpvarCollector = collectors.NewExpvarCollector(map[string]*prometheus.Desc{
	"badger_v3_disk_reads_total":     prometheus.NewDesc("badger_disk_reads_total", "Disk Reads", nil, nil),
	"badger_v3_disk_writes_total":    prometheus.NewDesc("badger_disk_writes_total", "Disk Writes", nil, nil),
	"badger_v3_read_bytes":           prometheus.NewDesc("badger_read_bytes", "Read bytes", nil, nil),
	"badger_v3_written_bytes":        prometheus.NewDesc("badger_written_bytes", "Written bytes", nil, nil),
	"badger_v3_lsm_level_gets_total": prometheus.NewDesc("badger_lsm_level_gets_total", "LSM Level Gets", []string{"level"}, nil),
	"badger_v3_lsm_bloom_hits_total": prometheus.NewDesc("badger_lsm_bloom_hits_total", "LSM Bloom Hits", []string{"level"}, nil),
	"badger_v3_gets_total":           prometheus.NewDesc("badger_gets_total", "Gets", nil, nil),
	"badger_v3_puts_total":           prometheus.NewDesc("badger_puts_total", "Puts", nil, nil),
	"badger_v3_blocked_puts_total":   prometheus.NewDesc("badger_blocked_puts_total", "Blocked Puts", nil, nil),
	"badger_v3_memtable_gets_total":  prometheus.NewDesc("badger_memtable_gets_total", "Memtable gets", nil, nil),
	"badger_v3_lsm_size_bytes":       prometheus.NewDesc("badger_lsm_size_bytes", "LSM Size in bytes", []string{"database"}, nil),
	"badger_v3_vlog_size_bytes":      prometheus.NewDesc("badger_vlog_size_bytes", "Value Log Size in bytes", []string{"database"}, nil),
	"badger_v3_pending_writes_total": prometheus.NewDesc("badger_pending_writes_total", "Pending Writes", []string{"database"}, nil),
	"badger_v3_compactions_current":  prometheus.NewDesc("badger_compactions_current", "Number of tables being compacted", nil, nil),
})

func init() {
	prometheus.MustRegister(badgerExpvarCollector)
}
