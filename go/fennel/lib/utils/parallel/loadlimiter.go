package parallel

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"runtime"
)

const OneCPU = 100.0

var AllCPUs = OneCPU * float64(runtime.NumCPU())

var semMap = make(map[string]*semaphore.Weighted)

func Book(name string, units float64) {
	sem := semMap[name]
	if sem == nil {
		panic(fmt.Sprintf("missing call of InitQuota for [%s]", name))
	}
	_ = sem.Acquire(context.Background(), int64(units))
}

func Release(name string, units float64) {
	sem := semMap[name]
	if sem == nil {
		panic(fmt.Sprintf("missing call of InitQuota for [%s]", name))
	}
	sem.Release(int64(units))
}

// InitQuota is not thread-safe with Book() and Release(), suggest to be called at the beginning of the app
func InitQuota(name string, totalUnits float64) {
	semMap[name] = semaphore.NewWeighted(int64(totalUnits))
}
