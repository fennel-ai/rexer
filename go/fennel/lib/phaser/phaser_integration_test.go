//go:build integration

package phaser

import (
	"context"
<<<<<<< HEAD
	"fennel/lib/value"
=======
	"fennel/s3"
>>>>>>> 9de3ff6b (Added unit tests and integration tests)
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
	tempFile, err := os.Create(PHASER_TMP_DIR + "/" + filename + REDIS_BULK_UPLOAD_FILE_SUFFIX)
	defer assert.NoError(t, err)

	rkeys := make([]string, 100000)
	for i := 0; i < 100000; i++ {
		tempFile.WriteString("SET key" + fmt.Sprint(i) + " value" + fmt.Sprint(i) + " EX 60\n")
		rkeys[i] = "key" + fmt.Sprint(i)
	}
	tempFile.Close()
	err = bulkUploadToRedis(tier, filename, 100000)
	assert.NoError(t, err)

	res, err := tier.Redis.MRawGet(ctx, rkeys...)
	assert.NoError(t, err)

	for i := 0; i < 100000; i++ {
		assert.Equal(t, "value"+fmt.Sprint(i), res[i])
	}
}

func TestPrepareAndBulkUpload(t *testing.T) {
	tier, err := test.Tier()
<<<<<<< HEAD
	assert.NoError(t, err)
	defer test.Teardown(tier)

	PHASER_TMP_DIR = "/tmp/aditya"

	err = tier.S3Client.BatchDiskDownload([]string{"integration-tests/item.parquet", "integration-tests/item2.parquet"}, S3Bucket, PHASER_TMP_DIR)
	assert.NoError(t, err)

	files := []string{"item.parquet"}
	p := Phaser{"testNamespace2", "testIdentifier2", "testBucket", "testPrefix", STRING, 1, time.Hour}
=======
	tier.ID = 123
	assert.NoError(t, err)
	defer test.Teardown(tier)

	s3Client := s3.NewClient(s3.S3Args{Region: "us-west-2"})
	err = s3Client.BatchDiskDownload([]string{"integration-tests/item.parquet", "integration-tests/item2.parquet"}, S3Bucket, PHASER_TMP_DIR)
	assert.NoError(t, err)

	files := []string{"item.parquet", "item2.parquet"}
	p := Phaser{"testNamespace", "testIdentifier", "testBucket", "testPrefix", STRING, 1, time.Hour}
>>>>>>> 9de3ff6b (Added unit tests and integration tests)
	err = p.prepareAndBulkUpload(tier, files)
	assert.NoError(t, err)

	// check that the files are in redis
<<<<<<< HEAD
	id := fmt.Sprint(tier.ID)
	rkeys := []string{id + ":testNamespace2:testIdentifier2:1:india", id + ":testNamespace2:testIdentifier2:1:russia", id + ":testNamespace2:testIdentifier2:1:usa"}
	res, err := tier.Redis.MRawGet(context.Background(), rkeys...)
	assert.NoError(t, err)
	assert.Equal(t, "ImFyanVuOjpzaHdldGhhOjpyYWh1bDo6YWRpdHlhOjphYmhheTo6bW9oaXQ6Om5pa2hpbDo6YXJheWEi", res[0])
	assert.Equal(t, "Im5hdGFzaGE6Om9sZWc6OnZvbG9keW15ciI=", res[1])
	assert.Equal(t, "ImpvaG46OnRpbTo6YmV0dHk6OmNsYWlyZTo6cGhpbCI=", res[2])
=======
	rkeys := []string{"123:testNamespace:testIdentifier:1:india", "123:testNamespace:testIdentifier:1:russia", "123:testNamespace:testIdentifier:1:usa"}
	res, err := tier.Redis.MRawGet(context.Background(), rkeys...)
	assert.NoError(t, err)
	assert.Equal(t, "ImFyanVufHNod2V0aGF8cmFodWx8YWRpdHlhfGFiaGF5fG1vaGl0fG5pa2hpbHxhcmF5YSI=", res[0])
	assert.Equal(t, "Im5hdGFzaGF8b2xlZ3x2b2xvZHlteXIi", res[1])
	assert.Equal(t, "ImpvaG58dGltfGJldHR5fGNsYWlyZXxwaGlsIg==", res[2])
>>>>>>> 9de3ff6b (Added unit tests and integration tests)
}

func TestPollS3Bucket(t *testing.T) {
	tier, err := test.Tier()
<<<<<<< HEAD
=======
	tier.ID = 123
>>>>>>> 9de3ff6b (Added unit tests and integration tests)
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

<<<<<<< HEAD
	keys := []string{"india", "russia", "usa"}
	namespaces := []string{"testNamespace", "testNamespace", "testNamespace"}
	identifiers := []string{"testIdentifier", "testIdentifier", "testIdentifier"}

	vals, err := BatchGet(tier, namespaces, identifiers, keys)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(vals))
	assert.Equal(t, value.String("arjun|shwetha|rahul|aditya|abhay|mohit|nikhil|araya"), vals[0])
	assert.Equal(t, value.String("natasha|oleg|volodymyr"), vals[1])
	assert.Equal(t, value.String("john|tim|betty|claire|phil"), vals[2])
=======
	rkeys := []string{"123:testNamespace:testIdentifier:1651531360:india", "123:testNamespace:testIdentifier:1651531360:russia", "123:testNamespace:testIdentifier:1651531360:usa"}
	res, err := tier.Redis.MRawGet(context.Background(), rkeys...)
	assert.NoError(t, err)
	assert.Equal(t, "ImFyanVufHNod2V0aGF8cmFodWx8YWRpdHlhfGFiaGF5fG1vaGl0fG5pa2hpbHxhcmF5YSI=", res[0])
	assert.Equal(t, "Im5hdGFzaGF8b2xlZ3x2b2xvZHlteXIi", res[1])
	assert.Equal(t, "ImpvaG58dGltfGJldHR5fGNsYWlyZXxwaGlsIg==", res[2])
>>>>>>> 9de3ff6b (Added unit tests and integration tests)

	currUpdateVersion, err := GetLatestUpdatedVersion(ctx, tier, "testNamespace", "testIdentifier")
	assert.NoError(t, err)
	assert.Equal(t, uint64(1651531360), currUpdateVersion)
}
