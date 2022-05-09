package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/model/aggregate"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/mtraver/base91"
)

type StatsArgs struct {
	SnapshotDir string   `arg:"--snapshot_dir,env:SNAPSHOT_DIR" json:"snapshot_dir,omitempty"`
	Aggregates  []uint32 `arg:"--aggregates,env:AGGREGATES" json:"aggregates,omitempty"`
}

type ShardStat struct {
	NumKeys   uint64
	SizeBytes uint64
	NumErrors uint32
}

func redisKeyPrefix(tr tier.Tier, aggId ftypes.AggId) (string, error) {
	aggBuf := make([]byte, 8) // aggId
	curr, err := binary.PutUvarint(aggBuf, uint64(aggId))
	if err != nil {
		return "", err
	}
	aggStr := base91.StdEncoding.EncodeToString(aggBuf[:curr])
	// TODO(mohit): redis key delimiter is hardcode, consider unifying this by making it a lib
	return fmt.Sprintf("%s-*", tr.Redis.Scope.PrefixedName(aggStr)), nil
}

func isRdbFile(f string) bool {
	return strings.HasSuffix(f, ".rdb")
}

func dirExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func createMemProfile(tr tier.Tier, aggId ftypes.AggId, snapshotDir string) error {
	csvDir := filepath.Join(snapshotDir, fmt.Sprintf("%d-csv", aggId))
	ok, err := dirExists(csvDir)
	if err != nil {
		panic(err)
	}
	if !ok {
		if err := os.Mkdir(csvDir, os.ModePerm); err != nil {
			panic(err)
		}
	}

	prefix, err := redisKeyPrefix(tr, aggId)
	if err != nil {
		return err
	}

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		panic(err)
	}

	// run a subprocess to create memory profile for given prefix
	wg := sync.WaitGroup{}

	// read the csv file and create the statistics from it
	csvs, err := os.ReadDir(csvDir)
	if err != nil {
		return err
	}

	// if the csv file already exists, do not compute them again
	for _, f := range files {
		if !f.IsDir() && isRdbFile(f.Name()) {
			wg.Add(1)
			go func(fileName string) {
				defer wg.Done()

				// check if the memory profile already exists
				csvFilePath := filepath.Join(csvDir, fmt.Sprintf("%d-%s.csv", aggId, strings.TrimSuffix(fileName, ".rdb")))
				if _, err := os.Stat(csvFilePath); !os.IsNotExist(err) {
					fmt.Printf("memory profile for agg: %d and snapshot: %s already exists, reusing it..\n", aggId, fileName)
					return
				}

				// generate memory profile and dump the csv file in snapshotDir/csv
				c := exec.Command("rdb", "-c", "memory", filepath.Join(snapshotDir, fileName), "--key", prefix, "-f", csvFilePath)
				var stderr bytes.Buffer
				c.Stderr = &stderr
				fmt.Printf("going to run: %s\n", c.String())
				err := c.Run()
				if err != nil {
					fmt.Printf("rdb command failed with: %v: %s", err, stderr.String())
				}
				fmt.Printf("finished memory profile for aggId: %d, rdb: %s\n", aggId, fileName)
			}(f.Name())
		}
	}
	wg.Wait()

	// read the files again, rdb could have created new files from the snapshot
	csvs, err = os.ReadDir(csvDir)
	if err != nil {
		return err
	}

	stats := make(chan ShardStat, len(csvs))

	for _, csvF := range csvs {
		wg.Add(1)
		go func(csvFile string) {
			defer wg.Done()
			f, err := os.Open(filepath.Join(csvDir, csvFile))
			if err != nil {
				fmt.Printf("failed to read the csv file: %v\n", err)
				stats <- ShardStat{}
				return
			}
			defer f.Close()
			csvReader := csv.NewReader(f)
			csvReader.FieldsPerRecord = -1   // this is required because our rediskeys might contain `,` and this will lead to uneven fields in a row
			csvReader.LazyQuotes = true      // similarly we can have `"` in the key as well
			data, err := csvReader.ReadAll() // [][]string
			if err != nil {
				fmt.Printf("failed to read contents from the csv file: %v\n", err)
				stats <- ShardStat{}
				return
			}

			// csv has the following format - database,type,key,size_in_bytes,encoding,num_elements,len_largest_element
			// size_in_bytes - includes the key, the value and any other overheads
			//
			// since the key here could be have `,`, we will fetch size_in_bytes from the end
			numEntries := 0
			totalSize := 0
			entryErrors := 0

			// skip the header so we start from 1:
			for _, d := range data[1:] {
				index := len(d) - 5
				if index <= 0 {
					fmt.Printf("error parsing size_in_bytes in %v\n", d)
					entryErrors += 1
					continue
				}
				size, err := strconv.Atoi(d[index])
				if err != nil {
					fmt.Printf("could not convert to int: %v\n", err)
					entryErrors += 1
					continue
				}
				numEntries++
				totalSize += size
			}
			// pipe these results back for aggregation
			stats <- ShardStat{NumKeys: uint64(numEntries), SizeBytes: uint64(totalSize), NumErrors: uint32(entryErrors)}
		}(csvF.Name())
	}
	wg.Wait()
	close(stats)

	totalKeys := 0
	totalSize := 0
	for stat := range stats {
		totalKeys += int(stat.NumKeys)
		totalSize += int(stat.SizeBytes)
	}

	fmt.Println("==========")
	fmt.Printf("[%d] total keys: %d, size (mb): %d\n", aggId, totalKeys, totalSize>>20)
	fmt.Println("==========")

	return nil
}

func main() {
	// seed random number generator so that all uses of rand work well
	rand.Seed(time.Now().UnixNano())
	// read db to fetch all aggregate ids or input ids
	var flags struct {
		tier.TierArgs
		StatsArgs
	}
	arg.MustParse(&flags)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(flags.SnapshotDir) == 0 {
		panic(fmt.Sprintf("--snapshot_dir is empty"))
	}

	// check if the file structure is correct
	files, err := os.ReadDir(flags.SnapshotDir)
	if err != nil {
		panic(err)
	}

	// check that the list of files returned is not empty
	if len(files) == 0 {
		panic("no snapshot files found in the given directory")
	}

	tier, err := tier.CreateFromArgs(&flags.TierArgs)
	if err != nil {
		panic(err)
	}

	// compute statistics from the snapshot files
	if len(flags.Aggregates) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(flags.Aggregates))
		for _, aggId := range flags.Aggregates {
			go func(aggId uint32) {
				defer wg.Done()
				if err := createMemProfile(tier, ftypes.AggId(aggId), flags.SnapshotDir); err != nil {
					tier.Logger.Info(fmt.Sprintf("memProfile failed for aggId: %d, failed with: %v", aggId, err))
				}
			}(aggId)
		}
		wg.Wait()
	} else {
		// create stats for all active aggregates
		aggs, err := aggregate.RetrieveAll(context.Background(), tier)
		if err != nil {
			panic(err)
		}
		wg := sync.WaitGroup{}
		wg.Add(len(aggs))
		for _, agg := range aggs {
			go func(aggId ftypes.AggId) {
				defer wg.Done()
				if err := createMemProfile(tier, aggId, flags.SnapshotDir); err != nil {
					tier.Logger.Info(fmt.Sprintf("memProfile failed for aggId: %d, failed with: %v", aggId, err))
				}
			}(agg.Id)
		}
		wg.Wait()
	}
}
