package gravel

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type manifestCodec uint64

const (
	mfile               = "gravel.manifest"
	v1    manifestCodec = 1
)

type Manifest struct {
	tableType TableType
	dirname   string
	numShards uint64
	tables    [][]Table
	lock      *sync.RWMutex
}

func InitManifest(dirname string, tableType TableType, numShards uint64) (*Manifest, error) {
	if err := numShardsValid(numShards); err != nil {
		return nil, err
	}
	// if the directory doesn't exist, create it
	if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
		return nil, err
	}

	mfileName := path.Join(dirname, mfile)
	fi, err := os.Stat(mfileName)
	if os.IsNotExist(err) || fi.Size() == 0 {
		if err = createEmpty(mfileName, numShards); err != nil {
			return nil, err
		}
		return &Manifest{
			tableType: tableType,
			dirname:   dirname,
			numShards: numShards,
			tables:    make([][]Table, numShards),
			lock:      &sync.RWMutex{},
		}, nil
	}
	f, err := os.Open(mfileName)
	if err != nil {
		return nil, fmt.Errorf("manifest file exists but could not open it: %w", err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	codec, err := nextInt(scanner)
	if err != nil || codec != uint64(v1) {
		return nil, fmt.Errorf("invalid codec: %d with error code: %w", codec, err)
	}
	curShards, err := nextInt(scanner)
	if err != nil {
		return nil, fmt.Errorf("could not read shard ID in manifest file: %w", err)
	}
	if err = numShardsValid(curShards); err != nil {
		return nil, fmt.Errorf("invalid shard ID in manifest file: %w", err)
	}
	manifest := &Manifest{
		tableType: tableType,
		dirname:   dirname,
		numShards: curShards,
		tables:    make([][]Table, curShards),
		lock:      &sync.RWMutex{},
	}
	seen := make(map[string]struct{})
	for i := 0; i < int(curShards) && scanner.Scan(); i++ {
		tables := make([]Table, 0)
		tablefiles := strings.Split(scanner.Text(), ",")
		for _, tablefile := range tablefiles {
			tablefile = strings.TrimSpace(tablefile) // remove any whitespaces in the name to make more robust
			id, err := validTableFileName(uint64(i), tablefile, false)
			if err != nil {
				return nil, fmt.Errorf("file '%s' has invalid file name: %w", tablefile, err)
			}
			if _, ok := seen[tablefile]; ok {
				return nil, fmt.Errorf("file '%s' appears mutliple times in manifest", tablefile)
			}
			seen[tablefile] = struct{}{}
			table, err := OpenTable(tableType, id, path.Join(dirname, tablefile))
			if err != nil {
				return nil, err
			}
			tables = append(tables, table)
		}
		manifest.tables[i] = tables
	}
	return manifest, nil
}

func (m *Manifest) List(shard uint64) ([]Table, error) {
	if shard >= m.numShards {
		return nil, fmt.Errorf("invalid shard ID")
	}
	return m.tables[uint(shard)], nil
}

func (m *Manifest) Append(filenames []string) error {
	if len(filenames) != int(m.numShards) {
		return fmt.Errorf("append expects one file for each shard but received %d files when there are %d shards", len(filenames), m.numShards)
	}
	for i, tablefile := range filenames {
		if _, err := validTableFileName(uint64(i), tablefile, true); err != nil {
			return fmt.Errorf("invalid table file: %s for shard: %d", tablefile, i)
		}
		nextID, err := m.nextID(uint64(i))
		if err != nil {
			return fmt.Errorf("could not find a valid ID for shard %d: %w", i, err)
		}
		oldpath := path.Join(m.dirname, tablefile)
		newName := fmt.Sprintf("%d_%d%s", i, nextID, SUFFIX)
		newPath := path.Join(m.dirname, newName)
		if err = os.Rename(oldpath, newPath); err != nil {
			return fmt.Errorf("could not rename file to appropriate name: %w", err)
		}
		filenames[i] = newName
	}
	m.lock.Lock()
	defer m.lock.Unlock()

	mfileName := path.Join(m.dirname, mfile)
	mfileNameTemp := path.Join(m.dirname, fmt.Sprintf("%s.tmp", mfile))
	err := func() error {
		f, err := os.Create(mfileNameTemp)
		if err != nil {
			return fmt.Errorf("could not create a temp file for updating manifest: %w", err)
		}
		defer f.Close()
		codecLine := fmt.Sprintf("%d\n", v1)
		shardLine := fmt.Sprintf("%d\n", m.numShards)
		if _, err = f.WriteString(codecLine + shardLine); err != nil {
			return fmt.Errorf("could not write to empty manifest file: %w", err)
		}
		for i := 0; i < int(m.numShards); i++ {
			tablefiles := make([]string, 0)
			for _, table := range m.tables[i] {
				tablefiles = append(tablefiles, fmt.Sprintf("%d_%d.grvl", i, table.ID()))
			}
			// move the new file to the front
			tablefiles = append([]string{filenames[i]}, tablefiles...)
			line := strings.Join(tablefiles, ",")
			if _, err = f.WriteString(fmt.Sprintf("%s\n", line)); err != nil {
				return fmt.Errorf("could not write table list to file: %w", err)
			}
		}
		if err = f.Sync(); err != nil {
			return fmt.Errorf("could not sync new manifest file: %w", err)
		}
		// now rename the manifest file and load it
		if err = os.Rename(mfileNameTemp, mfileName); err != nil {
			return fmt.Errorf("could not rename manifest file: %w", err)
		}
		return nil
	}()
	if err != nil {
		return fmt.Errorf("could not create temp manifest: %w", err)
	}
	newManifest, err := InitManifest(m.dirname, m.tableType, m.numShards)
	if err != nil {
		return fmt.Errorf("could not load new manifest after writing: %w", err)
	}
	m.tables = newManifest.tables
	return nil
}

// Replace replaces given filenames of the shard with a new single file
func (m *Manifest) Replace(shard uint, filenames []string, newfile string) error {
	panic("todo")
}

func (m *Manifest) Lock() {
	m.lock.RLock()
}

func (m *Manifest) Unlock() {
	m.lock.RUnlock()
}

// Clean removes all grvl.temp files from the directory as well as any .grvl files which are
// not listed in the manifest
func (m *Manifest) Clean() error {
	panic("todo")
}

func (m *Manifest) Close() error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i := 0; i < int(m.numShards); i += 1 {
		for _, t := range m.tables[i] {
			if err := t.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func createEmpty(filename string, numShards uint64) error {
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("manifest did not exist and could not create a new one: %w", err)
	}
	defer f.Close()
	// new manifest file - so just write the number of shards and move on
	codecLine := fmt.Sprintf("%d\n", v1)
	shardLine := fmt.Sprintf("%d\n", numShards)
	if _, err = f.WriteString(codecLine + shardLine); err != nil {
		return fmt.Errorf("could not write to empty manifest file: %w", err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("could not sync manifest file: %w", err)
	}
	return nil
}

func nextInt(s *bufio.Scanner) (uint64, error) {
	if !s.Scan() {
		return 0, fmt.Errorf("no more tokens left")
	}
	n, err := strconv.ParseUint(s.Text(), 10, 64)
	return n, err
}

func numShardsValid(n uint64) error {
	if n&(n-1) > 0 {
		return fmt.Errorf("num shards not a power of 2")
	}
	if n == 0 || n > 1024 {
		return fmt.Errorf("num shards can only be between 1 and 1024")
	}
	return nil
}

func validTableFileName(shard uint64, filename string, temp bool) (uint64, error) {
	suffix := SUFFIX
	if temp {
		suffix = tempSuffix
	}
	if !strings.HasSuffix(filename, suffix) {
		return 0, fmt.Errorf("table file doesn't end with expected suffix: %s", suffix)
	}
	if !strings.HasPrefix(filename, fmt.Sprintf("%d_", shard)) {
		return 0, fmt.Errorf("table file %s for shard: %d doesn't start with '%d_'", filename, shard, shard)
	}
	if temp {
		// we don't need IDs for temp files
		return 0, nil
	} else {
		parts := strings.Split(filename, "_")
		if len(parts) != 2 {
			return 0, fmt.Errorf("filename %s has more than one '_' character", filename)
		}
		parts = strings.Split(parts[1], ".")
		if len(parts) != 2 {
			return 0, fmt.Errorf("filename %s has more than one '.' character", filename)
		}
		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("filename doesn't have a valid 64 bit table ID: %w", err)
		}
		return id, nil
	}
}

func (m *Manifest) nextID(shard uint64) (uint64, error) {
	if shard >= m.numShards {
		return 0, fmt.Errorf("too large shard: %d", shard)
	}
	maxsofar := uint64(0)
	for _, t := range m.tables[uint(shard)] {
		id := t.ID()
		if id > maxsofar {
			maxsofar = id
		}
	}
	return 1 + maxsofar, nil
}
