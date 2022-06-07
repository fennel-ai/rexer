package main

import (
	"bytes"
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
	"fennel/lib/utils/encoding/base91"
	"fennel/tier"

	"github.com/alexflint/go-arg"
)

type StatsArgs struct {
	SnapshotDir string   `arg:"--snapshot_dir,env:SNAPSHOT_DIR" json:"snapshot_dir,omitempty"`
	Aggregates  []uint32 `arg:"--aggregates,env:AGGREGATES" json:"aggregates,omitempty"`
}

type ShardStat struct {
	NumKeys   uint64
	SizeBytes uint64
	ValLen    uint64
	KeyLen    uint64
	NumErrors uint32
}

func redisKeyPrefix(tr tier.Tier, aggId ftypes.AggId) (string, error) {
	aggBuf := make([]byte, 8) // aggId
	curr, err := binary.PutUvarint(aggBuf, uint64(aggId))
	if err != nil {
		return "", err
	}

	dest := make([]byte, 20) // this is not in the critical path, avoid using arena
	a, n := base91.StdEncoding.Encode(dest, aggBuf[:curr])
	// TODO(mohit): redis key delimiter is hardcode, consider unifying this by making it a lib
	return fmt.Sprintf("%s-*", tr.Redis.Scope.PrefixedName(a[:n])), nil
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

func createDir(dir string) error {
	ok, err := dirExists(dir)
	if err != nil {
		return err
	}
	if !ok {
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			return err
		}
	}
	return nil
}

func printAndLogStat(aggId ftypes.AggId, s ShardStat, csvWriter *csv.Writer) {
	avgKeyLen := float64(s.KeyLen) / float64(s.NumKeys)
	avgValLen := float64(s.ValLen) / float64(s.NumKeys)
	row := []string{strconv.Itoa(int(aggId)), strconv.Itoa(int(s.NumKeys)), fmt.Sprintf("%.2f", avgKeyLen), fmt.Sprintf("%.2f", avgValLen), strconv.Itoa(int(s.SizeBytes >> 20))}
	csvWriter.Write(row)

	fmt.Println("==========")
	fmt.Printf("AggId: %d\n", aggId)
	fmt.Printf("number of keys: %d\n", s.NumKeys)
	fmt.Printf("avg key length (NOTE: key is a string): %.2f\n", avgKeyLen)
	fmt.Printf("avg value length (NOTE: value is a string): %.2f\n", avgValLen)
	fmt.Printf("memory usage (MB): %d\n", s.SizeBytes>>20)
	fmt.Println("==========")
}

func computeStatsFromCsvs(aggId ftypes.AggId, csvDir string, csvWriter *csv.Writer) error {
	// read the files again, rdb could have created new files from the snapshot
	csvs, err := os.ReadDir(csvDir)
	if err != nil {
		return err
	}

	stats := make(chan ShardStat, len(csvs))

	wg := sync.WaitGroup{}

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
			stat := ShardStat{}

			// skip the header so we start from 1:
			for _, d := range data[1:] {
				index := len(d) - 5
				valLenIndex := len(d) - 3
				keyStartIndex := 2
				if index <= 0 {
					fmt.Printf("error parsing size_in_bytes in %v\n", d)
					stat.NumErrors += 1
					continue
				}
				k := strings.Join(d[keyStartIndex:index], ",")
				size, err := strconv.Atoi(d[index])
				if err != nil {
					fmt.Printf("could not convert (d[index]: %v) to int: %v\n", d[index], err)
					stat.NumErrors += 1
					continue
				}
				valLen, err := strconv.Atoi(d[valLenIndex])
				if err != nil {
					fmt.Printf("could not convert (d[valLenIndex]: %v) to int: %v\n", d[valLenIndex], err)
				}

				stat.NumKeys++
				stat.KeyLen += uint64(len(k))
				stat.SizeBytes += uint64(size)
				stat.ValLen += uint64(valLen)
			}
			// pipe these results back for aggregation
			stats <- stat
		}(csvF.Name())
	}
	wg.Wait()
	close(stats)

	stat := ShardStat{}
	for s := range stats {
		stat.NumKeys += s.NumKeys
		stat.KeyLen += s.KeyLen
		stat.NumErrors += s.NumErrors
		stat.ValLen += s.ValLen
		stat.SizeBytes += s.SizeBytes
	}

	printAndLogStat(aggId, stat, csvWriter)
	return nil
}

func createMemProfileForAgg(tr tier.Tier, aggId ftypes.AggId, snapshotDir string, csvWriter *csv.Writer) error {
	csvDir := filepath.Join(snapshotDir, fmt.Sprintf("%d-csv", aggId))
	if err := createDir(csvDir); err != nil {
		return err
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

	return computeStatsFromCsvs(aggId, csvDir, csvWriter)
}

func createMemProfileForSnapshot(tr tier.Tier, snapshotFile, csvFilePath string) error {
	// run the command
	c := exec.Command("rdb", "-c", "memory", snapshotFile, "-f", csvFilePath)
	var stderr bytes.Buffer
	c.Stderr = &stderr
	fmt.Printf("going to run: %s\n", c.String())
	err := c.Run()
	if err != nil {
		fmt.Printf("rdb command failed with: %v: %s", err, stderr.String())
	}
	return nil
}

func getAggId(key string, tierId int) (ftypes.AggId, error) {
	k := strings.TrimPrefix(key, fmt.Sprintf("t_%d_", tierId))
	// split using delimiter
	subs := strings.Split(k, "-")
	if len(subs) < 3 {
		// we probably came across dedup key :/
		return 0, nil
	}
	dest := make([]byte, 2 * len(key))
	n, err := base91.StdEncoding.Decode(dest, []byte(subs[0]))
	if err != nil {
		return 0, err
	}
	// get the aggId from the serialized bytes
	aggId, _, err := binary.ReadUvarint(dest[:n])
	if err != nil {
		return 0, err
	}
	return ftypes.AggId(aggId), nil
}

func computeStatsForAggs(csvDir, fileName string, tierId int) (map[ftypes.AggId]ShardStat, error) {
	f, err := os.Open(filepath.Join(csvDir, fileName))
	if err != nil {
		return nil, fmt.Errorf("failed to read the csv file: %v\n", err)
	}
	defer f.Close()

	// read the csv file
	csvReader := csv.NewReader(f)
	csvReader.FieldsPerRecord = -1   // this is required because our rediskeys might contain `,` and this will lead to uneven fields in a row
	csvReader.LazyQuotes = true      // similarly we can have `"` in the key as well
	data, err := csvReader.ReadAll() // [][]string
	if err != nil {
		return nil, fmt.Errorf("failed to read contents from the csv file: %v\n", err)
	}

	aggToStats := make(map[ftypes.AggId]ShardStat)

	for _, d := range data[1:] {
		// csv has the following format - database,type,key,size_in_bytes,encoding,num_elements,len_largest_element
		// size_in_bytes - includes the key, the value and any other overheads
		//
		// since the key here could be have `,`, we will fetch size_in_bytes from the end
		sizeIndex := len(d) - 5
		valLenIndex := len(d) - 3
		// keyIndex could be spread across more indices but we are only interested in the prefix
		//
		// the key will therefore be distributed in the range of indices - [2, len(d) - size_in_bytes), with each of them
		// concatenated by `,`
		keyStartIndex := 2
		if sizeIndex <= 0 {
			fmt.Printf("error parsing size_in_bytes in %v\n", d)
			continue
		}

		// construct key from [keyStartIndex, sizeIndex)
		key := strings.Join(d[keyStartIndex:sizeIndex], ",")
		aggId, err := getAggId(key, tierId)
		if err != nil {
			fmt.Printf("failed to parse aggId from the redisKey: %v, err: %v\n", key, err)
			continue
		}

		v, ok := aggToStats[aggId]
		if !ok {
			aggToStats[aggId] = ShardStat{}
		}

		// try converting the total memory usage to int
		size, err := strconv.Atoi(d[sizeIndex])
		if err != nil {
			v, _ := aggToStats[aggId]
			v.NumErrors++
			aggToStats[aggId] = v
			fmt.Printf("could not convert (v[sizeIndex]: %v) to int: %v\n", d[sizeIndex], err)
			continue
		}

		// try converting value length to int
		valLen, err := strconv.Atoi(d[valLenIndex])
		if err != nil {
			fmt.Printf("could not convert (v[valLenIdx]: %v) to int: %v\n", d[valLenIndex], err)
			continue
		}

		v, _ = aggToStats[aggId]
		v.NumKeys++
		v.SizeBytes += uint64(size)
		v.ValLen += uint64(valLen)
		v.KeyLen += uint64(len(key))
		aggToStats[aggId] = v
	}

	return aggToStats, nil
}

func createMemProfileForAggs(tr tier.Tier, snapshotDir string, csvWriter *csv.Writer) error {
	// create directory for csv files to be written to
	csvDir := filepath.Join(snapshotDir, "all-csvs")
	if err := createDir(csvDir); err != nil {
		return err
	}

	files, err := os.ReadDir(snapshotDir)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	// create csv file for each of the snapshot file concurrently
	for _, f := range files {
		// there could be directories with csv files
		if !f.IsDir() && isRdbFile(f.Name()) {
			wg.Add(1)
			go func(fileName string) {
				defer wg.Done()
				snapshotFile := filepath.Join(snapshotDir, fileName)
				csvFile := filepath.Join(csvDir, strings.Trim(fileName, ".rdb")+".csv")
				// check if the memory profile already exists
				if _, err := os.Stat(csvFile); !os.IsNotExist(err) {
					fmt.Printf("memory profile for snapshot: %s already exists, reusing it..\n", snapshotFile)
					return
				}

				// compute stats
				if err := createMemProfileForSnapshot(tr, snapshotFile, csvFile); err != nil {
					fmt.Printf("createMemProfileForSnapshot failed with: %v", err)
				}
			}(f.Name())
		}
	}
	wg.Wait()

	fmt.Print("memory profile generation completed, performing aggregation..\n")

	// read the csv files and compute per aggregate statistic
	csvFiles, err := os.ReadDir(csvDir)
	if err != nil {
		panic(err)
	}

	// for each csv file, compute statistics in a different goroutine
	chStats := make(chan map[ftypes.AggId]ShardStat, len(csvFiles))
	for _, f := range csvFiles {
		wg.Add(1)
		go func(fileName string) {
			defer wg.Done()
			stats, err := computeStatsForAggs(csvDir, fileName, int(tr.ID))
			if err != nil {
				fmt.Printf("computeStatsForAggs failed with: %v", err)
			}
			chStats <- stats
		}(f.Name())
	}
	wg.Wait()
	close(chStats)

	// aggregate over stats
	stats := make(map[ftypes.AggId]ShardStat, 100)
	for stat := range chStats {
		for aggId, s := range stat {
			v, ok := stats[aggId]
			if !ok {
				stats[aggId] = s
			} else {
				v.NumKeys += s.NumKeys
				v.SizeBytes += s.SizeBytes
				v.NumErrors += s.NumErrors
				v.KeyLen += s.KeyLen
				v.ValLen += s.ValLen
				stats[aggId] = v
			}
		}
	}
	for aggId, s := range stats {
		printAndLogStat(aggId, s, csvWriter)
	}
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
		panic("--snapshot_dir is empty")
	}

	// check if the file structure is correct and snapshot files do exist
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

	// write output to csv file
	csvFile, err := os.Create(filepath.Join(flags.SnapshotDir, "stats.csv"))
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	csvWriter.Write([]string{"AggId", "NumKeys", "Avg KeyLen", "Avg ValLen", "Total size MB"})

	// compute statistics from the snapshot files
	if len(flags.Aggregates) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(flags.Aggregates))
		for _, aggId := range flags.Aggregates {
			go func(aggId uint32) {
				defer wg.Done()
				if err := createMemProfileForAgg(tier, ftypes.AggId(aggId), flags.SnapshotDir, csvWriter); err != nil {
					fmt.Printf("createMemProfileForAgg failed for aggId: %d, failed with: %v", aggId, err)
				}
			}(aggId)
		}
		wg.Wait()
	} else {
		if err := createMemProfileForAggs(tier, flags.SnapshotDir, csvWriter); err != nil {
			fmt.Printf("createMemProfileForAggs failed with: %v", err)
		}
	}
}
