//go:build integration

package phaser

import (
	"context"
	"io/ioutil"

	"fennel/lib/value"
	"fennel/test"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBulkUploadToRedis(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	filename := "phaser_test.txt"
	tempDir, err := ioutil.TempDir("", "phaser")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)
	tempFile, err := os.Create(tempDir + "/" + filename + REDIS_BULK_UPLOAD_FILE_SUFFIX)
	defer assert.NoError(t, err)

	rkeys := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		tempFile.WriteString("SET key" + fmt.Sprint(i) + " value" + fmt.Sprint(i) + " EX 60\n")
		rkeys[i] = "key" + fmt.Sprint(i)
	}
	tempFile.Close()
	err = bulkUploadToRedis(tier, filename, 100000, tempDir)
	assert.NoError(t, err)

	res, err := tier.Redis.MRawGet(ctx, rkeys...)
	assert.NoError(t, err)

	for i := 0; i < 100000; i++ {
		assert.Equal(t, "value"+fmt.Sprint(i), res[i])
	}
}

func TestPrepareAndBulkUpload(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	tempDir, err := ioutil.TempDir("", "phaser")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	err = tier.S3Client.BatchDiskDownload([]string{"integration-tests/item.parquet", "integration-tests/item2.parquet"}, S3Bucket, tempDir)
	assert.NoError(t, err)

	files := []string{"item.parquet", "item2.parquet"}
	p := Phaser{"testNamespace2", "testIdentifier2", "testBucket", "testPrefix", STRING, 1, time.Hour}
	err = p.prepareAndBulkUpload(tier, files, tempDir)
	assert.NoError(t, err)

	// check that the files are in redis
	id := fmt.Sprint(tier.ID)
	rkeys := []string{id + ":testNamespace2:testIdentifier2:1:india", id + ":testNamespace2:testIdentifier2:1:russia", id + ":testNamespace2:testIdentifier2:1:usa"}
	res, err := tier.Redis.MRawGet(context.Background(), rkeys...)
	assert.NoError(t, err)
	assert.Equal(t, "ImFyanVufHNod2V0aGF8cmFodWx8YWRpdHlhfGFiaGF5fG1vaGl0fG5pa2hpbHxhcmF5YSI=", res[0])
	assert.Equal(t, "Im5hdGFzaGF8b2xlZ3x2b2xvZHlteXIi", res[1])
	assert.Equal(t, "ImpvaG58dGltfGJldHR5fGNsYWlyZXxwaGlsIg==", res[2])
}

func TestPollS3Bucket(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	err = NewPhaser("testNamespace", "testIdentifier", "phaser-test-data", "integration-tests", time.Minute*time.Duration(5), STRING, tier)
	assert.NoError(t, err)

	phasers, err := RetrieveAll(ctx, tier)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(phasers))
	pollS3Bucket("testNamespace", "testIdentifier", tier)

	POLL_FREQUENCY_SEC = 5
	time.Sleep(10 * time.Second)

	keys := []string{"india", "russia", "usa"}
	namespaces := []string{"testNamespace", "testNamespace", "testNamespace"}
	identifiers := []string{"testIdentifier", "testIdentifier", "testIdentifier"}

	vals, err := BatchGet(tier, namespaces, identifiers, keys)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, value.String("arjun|shwetha|rahul|aditya|abhay|mohit|nikhil|araya"), vals[0])
	assert.Equal(t, value.String("natasha|oleg|volodymyr"), vals[1])
	assert.Equal(t, value.String("john|tim|betty|claire|phil"), vals[2])

	currUpdateVersion, err := GetLatestUpdatedVersion(ctx, tier, "testNamespace", "testIdentifier")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1651531360), currUpdateVersion)
}
