package phaser

import (
	"context"
	"encoding/base64"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/tier"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var SUCCESS_PREFIX = "_SUCCESS-"

// parquet file is read in batches of BATCH_SIZE from disk.
var BATCH_SIZE = 1000
var REDIS_BULK_UPLOAD_FILE_SUFFIX = "-redis-bulk-upload.txt"
var POLL_FREQUENCY_SEC = 60

type PhaserSchema int

const (
	ITEM_SCORE_LIST PhaserSchema = iota
	ITEM_LIST
	STRING
)

func FromPhaserSchema(schema string) (PhaserSchema, error) {
	switch schema {
	case "ITEM_SCORE_LIST":
		return ITEM_SCORE_LIST, nil
	case "ITEM_LIST":
		return ITEM_LIST, nil
	case "STRING":
		return STRING, nil
	}
	return -1, fmt.Errorf("Unknown Phaser schema, currently we only support ITEM_SCORE_LIST,ITEM_LIST,  STRING")
}

func (schema PhaserSchema) String() (string, error) {
	switch schema {
	case ITEM_SCORE_LIST:
		return "ITEM_SCORE_LIST", nil
	case ITEM_LIST:
		return "ITEM_LIST", nil
	case STRING:
		return "STRING", nil
	}
	return "", fmt.Errorf("Unknown Phaser schema, currently we only support ITEM_SCORE_LIST,ITEM_LIST,  STRING")
}

//================================================
// Public API for Phaser
//================================================

type Phaser struct {
	// Namespace is used to group a bunch of phasers that logically belong together.
	// For eg. OFFLINE_AGG_NAMESPACE is the namespace for the phasers that are used for offline aggregate serving.
	Namespace string
	// Identifier is used to uniquely identify a phaser within a namespace.
	Identifier string
	// The S3 bucket where the data is stored
	S3Bucket string
	// Prefix withing the s3 bucket which is polled by Phaser.
	S3Prefix string
	// Schema of the data stored.
	Schema        PhaserSchema
	UpdateVersion uint64
	TTL           time.Duration
}

func NewPhaser(namespace, identifier, s3Bucket, s3Prefix string, ttl time.Duration, schema PhaserSchema, tr tier.Tier) error {
	_, err := GetLatestUpdatedVersion(context.Background(), tr, namespace, identifier)
	if err != nil && err == PhaserNotFound {
		return InitializePhaser(context.Background(), tr, s3Bucket, s3Prefix, namespace, identifier, ttl, schema)
	} else if err != nil {
		return err
	} else {
		return fmt.Errorf("Phaser in namespace %s & identifier %s already exists", namespace, identifier)
	}
}

func Get(namespace, identifier, key string) (interface{}, error) {
	return BatchGet(tier.Tier{}, []string{namespace}, []string{identifier}, []string{key})
}

func BatchGet(tr tier.Tier, namespaces, identifiers, keys []string) ([]value.Value, error) {
	phasers, err := RetrieveBatch(context.Background(), tr, namespaces, identifiers)
	if err != nil {
		return nil, err
	}

	// construct keys
	keysToGet := make([]string, 0, len(namespaces))
	for i := 0; i < len(namespaces); i++ {
		key := phasers[i].getRedisKey(tr.ID, keys[i])
		keysToGet = append(keysToGet, key)
	}

	res, err := tr.Redis.MRawGet(context.Background(), keysToGet...)
	if err != nil {
		return nil, err
	}

	// decode results
	results := make([]value.Value, 0, len(namespaces))
	for i := 0; i < len(namespaces); i++ {
		if res[i] == nil {
			results = append(results, nil)
		} else {
			deser, err := base64.StdEncoding.DecodeString(res[i].(string))
			if err != nil {
				return nil, err
			}
			val, err := value.FromJSON(deser)
			if err != nil {
				return nil, err
			}
			results = append(results, val)
		}
	}
	return results, nil
}

func DeletePhaser(tr tier.Tier, namespace, identifier string) error {
	return DelPhaser(context.Background(), tr, namespace, identifier)
}

func ServeData(tr tier.Tier, p Phaser) {
	pollS3Bucket(p.Namespace, p.Identifier, tr)
}

func (p Phaser) GetId() string {
	return p.Namespace + "::" + p.Identifier
}

//================================================
// Private helpers/interface
//================================================

// Different formats supported by Phaser include
// 1. Key -> List of ( item[string], score[double] )
// 2. Key -> List of item[string]
// 3. Key -> Item[string]
type ItemScore struct {
	ItemName *string  `parquet:"name=item, type=BYTE_ARRAY, convertedtype=UTF8"`
	Score    *float64 `parquet:"name=score, type=FLOAT"`
}

type ItemScoreList struct {
	Key           *string     `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8"`
	ItemScoreList []ItemScore `parquet:"name=item_list, type=LIST"`
}

type ItemList struct {
	Key      *string  `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8"`
	ItemList []string `parquet:"name=item_list, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

type Item struct {
	Key  *string `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8"`
	Item string  `parquet:"name=item, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func bulkUploadToRedis(tr tier.Tier, file string, numRows int, tempDir string) error {
	redisAddress := tr.Args.RedisServer[:strings.IndexByte(tr.Args.RedisServer, ':')]
	fetchClusterNodes := "redis-cli -c -h " + redisAddress + " --tls  cluster nodes | cut -f2 -d' '"
	out, err := exec.Command("bash", "-c", fetchClusterNodes).Output()
	if err != nil {
		return err
	}

	nodes := strings.Split(string(out), "\n")
	if len(nodes) == 0 {
		return fmt.Errorf("no redis cluster nodes found")
	}

	g, _ := errgroup.WithContext(context.Background())
	results := make([]int, len(nodes))
	for i, n := range nodes {
		idx := i
		nodeAddress := strings.TrimSpace(n)
		g.Go(func() error {
			node := strings.TrimSpace(nodeAddress)
			if !strings.Contains(node, ":") {
				return nil
			}
			nodeAddress := node[:strings.IndexByte(node, ':')]

			bulkUploadCmd := "cat " + tempDir + "/" + file + REDIS_BULK_UPLOAD_FILE_SUFFIX + " | redis-cli -h " + nodeAddress + " --pipe --tls"
			// We know it will error, so dont check the error

			out, err = exec.Command("bash", "-c", bulkUploadCmd).Output()
			if err != nil {
				return err
			}
			re := regexp.MustCompile(".*errors\\:\\s([0-9]+),\\sreplies\\:\\s([0-9]+)")
			match := re.FindStringSubmatch(string(out))
			if len(match) < 3 {
				return fmt.Errorf("could not identify number of successfull phaser writes to redis :- %s", string(out))
			}

			sentRequest, _ := strconv.Atoi(match[2])
			failedRequests, _ := strconv.Atoi(match[1])
			results[idx] = sentRequest - failedRequests
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	successfulRequests := 0
	for _, r := range results {
		successfulRequests += r
	}

	if successfulRequests != numRows {
		return fmt.Errorf("Could not write all rows successfully, %d / %d", successfulRequests, numRows)
	}
	return nil
}

func (p Phaser) getRedisKey(tierId ftypes.RealmID, key string) string {
	return fmt.Sprintf("%d:%s:%s:%d:%s", int(tierId), p.Namespace, p.Identifier, p.UpdateVersion, key)
}

func (p Phaser) getRedisCommand(tierId ftypes.RealmID, key, encodedString string) string {
	return "SET " + p.getRedisKey(tierId, key) + " " + encodedString + " EX " + fmt.Sprint(int(p.TTL.Seconds())) + "\n"
}

func (p Phaser) createItemScoreListFile(localFileReader source.ParquetFile, redisWriter *os.File, tierId ftypes.RealmID) (int, error) {
	pr, err := reader.NewParquetReader(localFileReader, new(ItemScoreList), 4)
	defer pr.ReadStop()
	if err != nil {
		return 0, nil
	}

	numRows := int(pr.GetNumRows())
	for i := 0; i < numRows; i += BATCH_SIZE {
		sz := BATCH_SIZE
		if i+BATCH_SIZE > numRows {
			sz = numRows - i
		}
		examples := make([]ItemScoreList, sz)
		if err = pr.Read(&examples); err != nil {
			return 0, fmt.Errorf("%w", err)
		}

		for _, example := range examples {
			v := value.NewList()
			for _, item := range example.ItemScoreList {
				if item.ItemName != nil {
					v.Append(value.NewDict(map[string]value.Value{
						"item":  value.String(*item.ItemName),
						"score": value.Double(*item.Score),
					}))
				}
			}
			encodedString := base64.StdEncoding.EncodeToString(value.ToJSON(v))
			redisWriter.WriteString(p.getRedisCommand(tierId, *example.Key, encodedString))
		}
	}
	return numRows, nil
}

func (p Phaser) createItemListFile(localFileReader source.ParquetFile, redisWriter *os.File, tierId ftypes.RealmID) (int, error) {
	pr, err := reader.NewParquetReader(localFileReader, new(ItemList), 4)
	defer pr.ReadStop()
	if err != nil {
		return 0, err
	}

	numRows := int(pr.GetNumRows())
	for i := 0; i < numRows; i += BATCH_SIZE {
		sz := BATCH_SIZE
		if i+BATCH_SIZE > numRows {
			sz = numRows - i
		}
		examples := make([]ItemList, sz)
		if err = pr.Read(&examples); err != nil {
			return 0, fmt.Errorf("%w", err)
		}

		for _, example := range examples {
			v := value.NewList()
			for _, item := range example.ItemList {
				if item != "" {
					v.Append(value.String(item))
				}
			}
			encodedString := base64.StdEncoding.EncodeToString(value.ToJSON(v))
			redisWriter.WriteString(p.getRedisCommand(tierId, *example.Key, encodedString))
		}
	}
	return numRows, nil
}

func (p Phaser) createItemFile(localFileReader source.ParquetFile, redisWriter *os.File, tierId ftypes.RealmID) (int, error) {
	pr, err := reader.NewParquetReader(localFileReader, new(Item), 4)
	defer pr.ReadStop()
	if err != nil {
		return 0, err
	}

	numRows := int(pr.GetNumRows())
	for i := 0; i < numRows; i += BATCH_SIZE {
		sz := BATCH_SIZE
		if i+BATCH_SIZE > numRows {
			sz = numRows - i
		}
		examples := make([]Item, sz)
		if err = pr.Read(&examples); err != nil {
			return 0, fmt.Errorf("%w", err)
		}

		for _, example := range examples {
			encodedString := base64.StdEncoding.EncodeToString(value.ToJSON(value.String(example.Item)))
			redisWriter.WriteString(p.getRedisCommand(tierId, *example.Key, encodedString))
		}
	}
	return numRows, nil
}

// This function is responsible for reading the parquet file from
// tempDir and creating the appropriate redis file for uploading.
func (p Phaser) prepareFileForBulkUpload(tr tier.Tier, file string, tempDir string) (int, error) {
	localFileReader, err := local.NewLocalFileReader(tempDir + "/" + file)
	if err != nil {
		return 0, err
	}
	defer localFileReader.Close()
	redisWriter, err := os.Create(tempDir + "/" + file + REDIS_BULK_UPLOAD_FILE_SUFFIX)
	if err != nil {
		return 0, err
	}
	defer redisWriter.Close()

	switch p.Schema {
	case ITEM_SCORE_LIST:
		return p.createItemScoreListFile(localFileReader, redisWriter, tr.ID)
	case ITEM_LIST:
		return p.createItemListFile(localFileReader, redisWriter, tr.ID)
	case STRING:
		return p.createItemFile(localFileReader, redisWriter, tr.ID)
	default:
		return 0, fmt.Errorf("unknown schema type: %d", p.Schema)
	}
}

func (p Phaser) prepareAndBulkUpload(tr tier.Tier, fileNames []string, tempDir string) error {
	g, _ := errgroup.WithContext(context.Background())

	for _, f := range fileNames {
		file := f
		g.Go(func() error {
			numRows, err := p.prepareFileForBulkUpload(tr, file, tempDir)
			if err != nil {
				return err
			}

			err = bulkUploadToRedis(tr, file, numRows, tempDir)
			if err != nil {
				return err
			}
			return nil
		})
	}
	return g.Wait()
}

func findLatestVersion(files []string, currUpdateVersion uint64) (uint64, string, error) {
	var prefixToUpdate string
	for _, file := range files {
		pathArray := strings.Split(file, "/")
		if len(pathArray) > 0 && strings.HasPrefix(pathArray[len(pathArray)-1], SUCCESS_PREFIX) {
			updateVersion := strings.Replace(pathArray[len(pathArray)-1], SUCCESS_PREFIX, "", 1)
			UpdateVersionInt, err := strconv.ParseUint(updateVersion, 10, 64)
			if err != nil {
				return 0, "", err
			}
			if UpdateVersionInt > currUpdateVersion {
				prefixToUpdate = strings.Join(pathArray[:len(pathArray)-1], "/")
				currUpdateVersion = UpdateVersionInt
			}
		}
	}

	// Couldn't find any new updates, so return
	if prefixToUpdate == "" {
		return 0, "", nil
	}

	return currUpdateVersion, prefixToUpdate + "/", nil
}

func (p Phaser) updateServing(tr tier.Tier, fileNames, filesToDownload []string, newUpdateVersion uint64) error {
	tempDir, err := ioutil.TempDir("", "phaser")
	if err != nil {
		return err
	}

	defer os.RemoveAll(tempDir)

	err = tr.S3Client.BatchDiskDownload(filesToDownload, p.S3Bucket, tempDir)
	if err != nil {
		tr.Logger.Error("error while downloading files from s3 bucket:", zap.Error(err))
		return err
	}

	err = p.prepareAndBulkUpload(tr, fileNames, tempDir)
	if err != nil {
		tr.Logger.Error("error during bulk upload phaser data to redis", zap.Error(err))
		return err
	}

	// Update DB with the new version
	err = UpdateVersion(context.Background(), tr, p.Namespace, p.Identifier, newUpdateVersion)
	if err != nil {
		tr.Logger.Error("error while updating aggregate version:", zap.Error(err))
		return err
	}
	return nil
}

func pollS3Bucket(namespace, identifier string, tr tier.Tier) error {
	go func(tr tier.Tier, namespace, identifier string) {
		ticker := time.NewTicker(time.Second * time.Duration(POLL_FREQUENCY_SEC))
		for {
			<-ticker.C
			p, err := Retrieve(context.Background(), tr, namespace, identifier)
			if err != nil {
				tr.Logger.Error("Error retrieving phaser", zap.Error(err))
				continue
			}

			tr.Logger.Info("Processing phaser ", zap.String("ID", p.GetId()))
			if err != nil {
				tr.Logger.Error("failed to get latest updated version", zap.Error(err))
				continue
			}

			files, err := tr.S3Client.ListFiles(p.S3Bucket, p.S3Prefix)
			if err != nil {
				tr.Logger.Error("error while listing files in s3 bucket:", zap.Error(err))
				continue
			}

			newUpdateVersion, prefixToUpdate, err := findLatestVersion(files, p.UpdateVersion)

			if err != nil {
				tr.Logger.Error("error while findLatestVersion ", zap.Error(err))
				continue
			}

			if newUpdateVersion <= p.UpdateVersion {
				tr.Logger.Info("No new updates found for ", zap.String("ID", p.GetId()))
				continue
			}

			p.UpdateVersion = newUpdateVersion

			var filesToDownload []string
			var fileNames []string

			for _, file := range files {
				if file != prefixToUpdate && strings.HasPrefix(file, prefixToUpdate) && !strings.HasSuffix(file, fmt.Sprintf("%s%d", SUCCESS_PREFIX, newUpdateVersion)) {
					filesToDownload = append(filesToDownload, file)
					fileNames = append(fileNames, strings.Replace(file, prefixToUpdate, "", 1))
				}
			}

			err = p.updateServing(tr, fileNames, filesToDownload, newUpdateVersion)
			if err != nil {
				tr.Logger.Error("error while updating serving", zap.Error(err))
			}
		}
	}(tr, namespace, identifier)
	return nil
}
