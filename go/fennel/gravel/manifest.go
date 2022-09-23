package gravel

import (
	"bufio"
	"fmt"
	"go.uber.org/zap"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

type manifestCodec uint64

const (
	mfile                     = "gravel.manifest"
	v1          manifestCodec = 1
	maxShardNum               = 65536 // 65536 is an arbitrary limit, since we unlikely will need more than this
)

// Manifest object has no concurrency guarantee. Its owner is supposed to handle race condition.
type Manifest struct {
	tableType   TableType
	dirname     string
	numShards   uint64
	tableFiles  [][]string
	maxTableIDs []uint64
}

// validates the list of tablefile names, and returns the sorted names based on ID, and the max existing ID.
func validateAndSortTableFiles(tableFiles []string, shardId uint64) ([]string, uint64, error) {
	type entry struct {
		id   uint64
		name string
	}

	var entries []entry = nil
	seen := make(map[string]struct{})
	for _, tableFile := range tableFiles {
		tableFile = strings.TrimSpace(tableFile) // remove any whitespaces in the name to make more robust
		id, _, err := validTableFileName(shardId, tableFile, false)
		if err != nil {
			return nil, 0, fmt.Errorf("file '%s' has invalid file name: %w", tableFile, err)
		}
		if _, ok := seen[tableFile]; ok {
			return nil, 0, fmt.Errorf("file '%s' appears mutliple times in manifest", tableFile)
		}
		seen[tableFile] = struct{}{}
		entries = append(entries, entry{id: id, name: tableFile})
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].id < entries[j].id
	})

	sortedTableNames := make([]string, len(entries))
	maxID := uint64(0)
	for idx, item := range entries {
		sortedTableNames[idx] = item.name
		if item.id > maxID {
			maxID = item.id
		}
	}
	return sortedTableNames, maxID, nil
}

func InitManifest(dirname string, tableType TableType, numShards uint64) (*Manifest, error) {
	fileName := path.Join(dirname, mfile)
	fi, err := os.Stat(fileName)
	if os.IsNotExist(err) || fi.Size() == 0 {
		if err = createEmpty(fileName, numShards, tableType); err != nil {
			return nil, err
		}
		return &Manifest{
			tableType:   tableType,
			dirname:     dirname,
			numShards:   numShards,
			tableFiles:  make([][]string, numShards),
			maxTableIDs: make([]uint64, numShards),
		}, nil
	}
	f, err := os.Open(fileName)
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
	tableTypeInt, err := nextInt(scanner)
	tableType = TableType(tableTypeInt)
	if err != nil || tableType >= InvalidTable {
		return nil, fmt.Errorf("invalid table type: %d with error code: %w", tableType, err)
	}
	curShards, err := nextInt(scanner)
	if err != nil {
		return nil, fmt.Errorf("could not read shard ID in manifest file: %w", err)
	}
	if err = numShardsValid(curShards); err != nil {
		return nil, fmt.Errorf("invalid shard ID in manifest file: %w", err)
	}
	manifest := &Manifest{
		tableType:   tableType,
		dirname:     dirname,
		numShards:   curShards,
		tableFiles:  make([][]string, curShards),
		maxTableIDs: make([]uint64, curShards),
	}

	for i := 0; i < int(curShards) && scanner.Scan(); i++ {
		tableFiles := strings.Split(scanner.Text(), ",")
		tableFiles, maxID, err := validateAndSortTableFiles(tableFiles, uint64(i))
		if err != nil {
			return nil, err
		}
		manifest.tableFiles[i] = tableFiles
		manifest.maxTableIDs[i] = maxID
	}
	return manifest, nil
}

// GetTableFiles lists all the tables of a shard in the order of ID
func (m *Manifest) GetTableFiles(shard uint64) ([]string, error) {
	if shard >= m.numShards {
		return nil, fmt.Errorf("invalid shard ID")
	}
	return m.tableFiles[shard], nil
}

// creates a new manifest file with changes. Both toAdd and toDelete can be nil if there is no corresponding change
func (m *Manifest) writeAndLoadNewManifest(toAdd map[uint64][]string, toDelete map[uint64][]string) error {
	mfileName := path.Join(m.dirname, mfile)
	mfileNameTemp := path.Join(m.dirname, fmt.Sprintf("%s.tmp", mfile))
	f, err := os.Create(mfileNameTemp)
	if err != nil {
		return fmt.Errorf("could not create a temp file for updating manifest: %w", err)
	}
	defer f.Close()

	metaLine := fmt.Sprintf("%d\n%d\n%d\n", v1, m.tableType, m.numShards)
	if _, err = f.WriteString(metaLine); err != nil {
		return fmt.Errorf("could not write to empty manifest file: %w", err)
	}
	for i := uint64(0); i < m.numShards; i++ {
		tableFilesMap := make(map[string]struct{})
		for _, tableFile := range m.tableFiles[i] {
			tableFilesMap[tableFile] = struct{}{}
		}
		for _, tableFile := range toAdd[i] {
			tableFilesMap[tableFile] = struct{}{}
		}
		for _, tableFile := range toDelete[i] {
			delete(tableFilesMap, tableFile)
		}

		tableFiles := make([]string, len(tableFilesMap))
		j := 0
		for k := range tableFilesMap {
			tableFiles[j] = k
			j++
		}
		line := strings.Join(tableFiles, ",")
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

	newManifest, err := InitManifest(m.dirname, m.tableType, m.numShards)
	if err != nil {
		return fmt.Errorf("could not load new manifest after writing: %w", err)
	}
	m.tableFiles = newManifest.tableFiles
	m.maxTableIDs = newManifest.maxTableIDs
	return nil
}

func (m *Manifest) Append(filenames []string) error {
	if len(filenames) != int(m.numShards) {
		return fmt.Errorf("append expects one file for each shard but received %d files when there are %d shards", len(filenames), m.numShards)
	}

	filesToAdd := make(map[uint64][]string)
	for i, tablefile := range filenames {
		if len(tablefile) == 0 {
			// empty file name indicates no such file
			continue
		}
		_, tableSuffix, err := validTableFileName(uint64(i), tablefile, true)
		if err != nil {
			return fmt.Errorf("invalid table file: %s for shard: %d", tablefile, i)
		}
		nextID, err := m.nextID(uint64(i))
		if err != nil {
			return fmt.Errorf("could not find a valid ID for shard %d: %w", i, err)
		}
		oldpath := path.Join(m.dirname, tablefile)
		newName := fmt.Sprintf("%d_%d_%s%s", i, nextID, tableSuffix, FileExtension)
		newPath := path.Join(m.dirname, newName)
		if err = os.Rename(oldpath, newPath); err != nil {
			return fmt.Errorf("could not rename file to appropriate name: %w", err)
		}
		filesToAdd[uint64(i)] = []string{newName}
	}
	if len(filesToAdd) > 0 {
		err := m.writeAndLoadNewManifest(filesToAdd, nil)
		if err != nil {
			return fmt.Errorf("failed to write new and reload manifest file %w", err)
		}
	}

	return nil
}

// Replace replaces given filenames of the shard with a new single file
func (m *Manifest) Replace(shardId uint64, filenames []string, newTmpFile string) error {
	id, _, err := validTableFileName(shardId, filenames[0], false) // compacted table use the ID of the first one of existing IDs
	if err != nil {
		return err
	}

	_, tableSuffix, err := validTableFileName(shardId, newTmpFile, true)
	if err != nil {
		return err
	}

	oldpath := path.Join(m.dirname, newTmpFile)
	newName := fmt.Sprintf("%d_%d_%s%s", shardId, id, tableSuffix, FileExtension)
	newPath := path.Join(m.dirname, newName)
	if err = os.Rename(oldpath, newPath); err != nil {
		return fmt.Errorf("could not rename file to appropriate name: %w", err)
	}

	toAdd := make(map[uint64][]string)
	toDelete := make(map[uint64][]string)
	toAdd[shardId] = []string{newName}
	toDelete[shardId] = filenames

	err = m.writeAndLoadNewManifest(toAdd, toDelete)
	if err != nil {
		return fmt.Errorf("failed to write new and reload manifest file %w", err)
	}

	for _, fileName := range filenames {
		fullName := path.Join(m.dirname, fileName)
		err := os.Remove(fullName)
		if err != nil {
			// ignorable error
			zap.L().Error("failed to remove old immutable file", zap.String("filename", fullName), zap.Error(err))
		}
	}

	return nil
}

// Clean removes all grvl.temp files from the directory as well as any .grvl files which are
// not listed in the manifest
func (m *Manifest) Clean() error {
	panic("todo")
}

func createEmpty(filename string, numShards uint64, tableType TableType) error {
	if err := numShardsValid(numShards); err != nil {
		return err
	}
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("manifest did not exist and could not create a new one: %w", err)
	}
	defer f.Close()
	// new manifest file - so just write the number of shards and move on
	metaLine := fmt.Sprintf("%d\n%d\n%d\n", v1, tableType, numShards)
	if _, err = f.WriteString(metaLine); err != nil {
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
	if n == 0 || n > maxShardNum {
		return fmt.Errorf("num shards can only be between 1 and 65536")
	}
	return nil
}

// validTableFileName checks and returns the shardId and suffix name, given table file name, regardless it's temp file or not
func validTableFileName(shard uint64, filename string, temp bool) (uint64, string, error) {
	suffix := FileExtension
	if temp {
		suffix = tempFileExtension
	}
	if !strings.HasSuffix(filename, suffix) {
		return 0, "", fmt.Errorf("table file doesn't end with expected suffix: %s", suffix)
	}
	if !strings.HasPrefix(filename, fmt.Sprintf("%d_", shard)) {
		return 0, "", fmt.Errorf("table file %s for shard: %d doesn't start with '%d_'", filename, shard, shard)
	}
	parts := strings.Split(filename, "_")
	if temp {
		// 15_1663730490016346.grvl
		if len(parts) != 2 {
			return 0, "", fmt.Errorf("filename %s has more than one '_' character", filename)
		}
		parts = strings.Split(parts[1], ".")
		// we don't have ID, but have suffix name, for temp files
		return 0, parts[0], nil
	} else {
		// 15_1_1663730490016346.grvl
		if len(parts) != 3 {
			return 0, "", fmt.Errorf("filename %s has more than one '_' character", filename)
		}
		id, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0, "", fmt.Errorf("filename doesn't have a valid 64 bit table ID: %w", err)
		}
		parts = strings.Split(parts[2], ".")
		if len(parts) != 2 {
			return 0, "", fmt.Errorf("filename %s has more than one '.' character", filename)
		}
		return id, parts[0], nil
	}
}

func (m *Manifest) nextID(shard uint64) (uint64, error) {
	if shard >= m.numShards {
		return 0, fmt.Errorf("too large shard: %d", shard)
	}
	return 1 + m.maxTableIDs[shard], nil
}
