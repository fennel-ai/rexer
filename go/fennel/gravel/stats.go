package gravel

import (
	"time"

	"github.com/detailyang/fastrand-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
)

const (
	sampleRate = 128
)

type Stats struct {
	Gets            atomic.Uint64
	Misses          atomic.Uint64
	MemtableHits    atomic.Uint64
	MemtableMisses  atomic.Uint64
	TableIndexReads atomic.Uint64

	// write related metrics
	Sets              atomic.Uint64
	Dels              atomic.Uint64
	Commits           atomic.Uint64
	MemtableSizeBytes atomic.Uint64
	MemtableKeys      atomic.Uint64
	NumTableBuilds    atomic.Uint64
	NumTables         atomic.Uint64
}

var stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "gravel_stats",
	Help: "Stats about performance of gravel",
}, []string{"metric", "name"})

func (g *Gravel) reportStats() {
	name := g.opts.Name
	for range time.Tick(10 * time.Second) {
		func() {
			stats.WithLabelValues("hits", name).Set(float64(g.stats.Gets.Load() * sampleRate))
			stats.WithLabelValues("misses", name).Set(float64(g.stats.Misses.Load() * sampleRate))
			stats.WithLabelValues("memtable_hits", name).Set(float64(g.stats.MemtableHits.Load() * sampleRate))
			stats.WithLabelValues("memtable_misses", name).Set(float64(g.stats.MemtableMisses.Load() * sampleRate))
			stats.WithLabelValues("table_index_reads", name).Set(float64(g.stats.TableIndexReads.Load() * sampleRate))
			stats.WithLabelValues("sets", name).Set(float64(g.stats.Sets.Load() * sampleRate))
			stats.WithLabelValues("dels", name).Set(float64(g.stats.Dels.Load() * sampleRate))
			stats.WithLabelValues("commits", name).Set(float64(g.stats.Commits.Load() * sampleRate))

			stats.WithLabelValues("num_tables", name).Set(float64(g.stats.NumTables.Load()))
			stats.WithLabelValues("num_table_builds", name).Set(float64(g.stats.NumTableBuilds.Load()))
			stats.WithLabelValues("memtable_size_bytes", name).Set(float64(g.stats.MemtableSizeBytes.Load()))
			stats.WithLabelValues("memtable_keys", name).Set(float64(g.stats.MemtableKeys.Load()))

			g.tableListLock.RLock()
			defer g.tableListLock.RUnlock()
			reads := uint64(0)
			for _, t := range g.tableList {
				reads += t.DataReads()
			}
			stats.WithLabelValues("table_data_reads", name).Set(float64(reads))
		}()
	}
}

func maybeInc(shouldSample bool, a *atomic.Uint64) {
	if shouldSample {
		a.Inc()
	}
}

func shouldSample() bool {
	return (fastrand.FastRand() & (sampleRate - 1)) == 0
}
