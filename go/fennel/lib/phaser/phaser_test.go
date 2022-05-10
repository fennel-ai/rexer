package phaser

import (
	"bufio"
	"fennel/s3"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
)

var S3Bucket = "phaser-test-data"

func TestCreateItemScoreListFile(t *testing.T) {
	itemListPq := "item_score_list.parquet"
	s3Client := s3.NewClient(s3.S3Args{Region: "us-west-2"})
	tempDir, err := ioutil.TempDir("", "phaser")
	defer os.RemoveAll(tempDir)
	assert.NoError(t, err)
	err = s3Client.BatchDiskDownload([]string{"unit-tests/" + itemListPq}, S3Bucket, tempDir)
	assert.NoError(t, err)
	localFileReader, err := local.NewLocalFileReader(tempDir + "/" + itemListPq)
	defer localFileReader.Close()
	assert.NoError(t, err)
	writeFile := fmt.Sprint(rand.Uint64()) + ".txt"
	cmdWriter, err := os.Create(tempDir + "/" + writeFile)
	defer cmdWriter.Close()

	assert.NoError(t, err)

	p := Phaser{"testNamespace", "testIdentifier", "testBucket", "testPrefix", ITEM_SCORE_LIST, 1, time.Hour}
	numRows, err := p.createItemScoreListFile(localFileReader, cmdWriter, 123)
	assert.NoError(t, err)
	assert.Equal(t, 3, numRows)

	file, err := os.Open(tempDir + "/" + writeFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	expected := []string{"SET 123:testNamespace:testIdentifier:1:ImluZGlhIg== W3siaXRlbSI6Im1vaGl0Iiwic2NvcmUiOjAuNTc3NTkyNTU3NjU4OTAxOH0seyJpdGVtIjoiYWJoYXkiLCJzY29yZSI6MC45NDc2MDQ3ODY5ODgwOTI1fSx7Iml0ZW0iOiJhZGl0eWEiLCJzY29yZSI6MC4zNjY2NDIyMjYxNzk0NzgxN30seyJpdGVtIjoiYXJheWEiLCJzY29yZSI6MC44MDc4Njg4MTc4MzcxODgyfSx7Iml0ZW0iOiJzaHdldGhhIiwic2NvcmUiOjAuNzEzNTE0MzQzMzQ1MjQ2MX0seyJpdGVtIjoiYXJqdW4iLCJzY29yZSI6MC44MDYyNTAzNzEyMDI1NzI2fSx7Iml0ZW0iOiJuaWtoaWwiLCJzY29yZSI6MC44MTMzMzA0ODAzODM3NjY3fSx7Iml0ZW0iOiJyYWh1bCIsInNjb3JlIjowLjUyNDcyODA5NjI5Mzg2NX1d EX 3600",
		"SET 123:testNamespace:testIdentifier:1:InJ1c3NpYSI= W3siaXRlbSI6Im9sZWciLCJzY29yZSI6MC4xNzA5NDk3MTM3OTU1NTY4fSx7Iml0ZW0iOiJ2b2xvZHlteXIiLCJzY29yZSI6MC44MDUxMTQzOTU4MDA1NDU5fSx7Iml0ZW0iOiJuYXRhc2hhIiwic2NvcmUiOjAuNzE5NTMyNTU2NjMwNjA1M31d EX 3600",
		"SET 123:testNamespace:testIdentifier:1:InVzYSI= W3siaXRlbSI6ImJldHR5Iiwic2NvcmUiOjAuMjA5MzcwNDk3NzU3N30seyJpdGVtIjoidGltIiwic2NvcmUiOjAuMzEzMzUyOTIzMTExNzU0NTZ9LHsiaXRlbSI6ImNsYWlyZSIsInNjb3JlIjowLjEwODE0OTE0NjQ2MTc2NjU0fSx7Iml0ZW0iOiJqb2huIiwic2NvcmUiOjAuMzM2MjIzMjk4MDcwMTE3Mn0seyJpdGVtIjoicGhpbCIsInNjb3JlIjowLjQ3NjQ5NDI4NzM4MTcwODk2fV0= EX 3600"}
	i := 0
	for scanner.Scan() {
		assert.Equal(t, expected[i], scanner.Text())
		i += 1
	}
}
func TestCreateItemListFile(t *testing.T) {
	itemListPq := "item_list.parquet"
	s3Client := s3.NewClient(s3.S3Args{Region: "us-west-2"})
	tempDir, err := ioutil.TempDir("", "phaser")
	defer os.RemoveAll(tempDir)
	assert.NoError(t, err)
	err = s3Client.BatchDiskDownload([]string{"unit-tests/" + itemListPq}, S3Bucket, tempDir)
	assert.NoError(t, err)
	localFileReader, err := local.NewLocalFileReader(tempDir + "/" + itemListPq)
	defer localFileReader.Close()
	assert.NoError(t, err)
	writeFile := fmt.Sprint(rand.Uint64()) + ".txt"
	cmdWriter, err := os.Create(tempDir + "/" + writeFile)
	defer cmdWriter.Close()

	assert.NoError(t, err)

	p := Phaser{"testNamespace", "testIdentifier", "testBucket", "testPrefix", ITEM_LIST, 1, time.Hour}
	numRows, err := p.createItemListFile(localFileReader, cmdWriter, 123)
	assert.NoError(t, err)
	assert.Equal(t, 3, numRows)

	file, err := os.Open(tempDir + "/" + writeFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	expected := []string{"SET 123:testNamespace:testIdentifier:1:ImluZGlhIg== WyJhcmp1biIsInNod2V0aGEiLCJyYWh1bCIsImFkaXR5YSIsImFiaGF5IiwibW9oaXQiLCJuaWtoaWwiLCJhcmF5YSJd EX 3600",
		"SET 123:testNamespace:testIdentifier:1:InJ1c3NpYSI= WyJuYXRhc2hhIiwib2xlZyIsInZvbG9keW15ciJd EX 3600",
		"SET 123:testNamespace:testIdentifier:1:InVzYSI= WyJqb2huIiwidGltIiwiYmV0dHkiLCJjbGFpcmUiLCJwaGlsIl0= EX 3600"}
	i := 0
	for scanner.Scan() {
		assert.Equal(t, expected[i], scanner.Text())
		i += 1
	}
}

func TestCreateItemFile(t *testing.T) {
	itemListPq := "item.parquet"
	s3Client := s3.NewClient(s3.S3Args{Region: "us-west-2"})
	tempDir, err := ioutil.TempDir("", "phaser")
	defer os.RemoveAll(tempDir)
	err = s3Client.BatchDiskDownload([]string{"unit-tests/" + itemListPq}, S3Bucket, tempDir)
	assert.NoError(t, err)
	localPqReader, err := local.NewLocalFileReader(tempDir + "/" + itemListPq)
	assert.NoError(t, err)
	defer localPqReader.Close()
	writeFile := fmt.Sprint(rand.Uint64()) + ".txt"
	cmdWriter, err := os.Create(tempDir + "/" + writeFile)
	defer cmdWriter.Close()

	assert.NoError(t, err)

	p := Phaser{"testNamespace", "testIdentifier", "testBucket", "testPrefix", STRING, 1, time.Hour}
	numRows, err := p.createItemFile(localPqReader, cmdWriter, 123)
	assert.NoError(t, err)
	assert.Equal(t, 3, numRows)

	file, err := os.Open(tempDir + "/" + writeFile)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	expected := []string{"SET 123:testNamespace:testIdentifier:1:ImluZGlhIg== ImFyanVuOjpzaHdldGhhOjpyYWh1bDo6YWRpdHlhOjphYmhheTo6bW9oaXQ6Om5pa2hpbDo6YXJheWEi EX 3600",
		"SET 123:testNamespace:testIdentifier:1:InJ1c3NpYSI= Im5hdGFzaGE6Om9sZWc6OnZvbG9keW15ciI= EX 3600",
		"SET 123:testNamespace:testIdentifier:1:InVzYSI= ImpvaG46OnRpbTo6YmV0dHk6OmNsYWlyZTo6cGhpbCI= EX 3600"}
	i := 0
	for scanner.Scan() {
		assert.Equal(t, expected[i], scanner.Text())
		i += 1
	}
}

func TestFindLatestVersion(t *testing.T) {

	files := []string{
		"folder1/folder2/folder2/item1.parquet",
		"folder1/folder2/folder2/item2.parquet",
		"folder1/folder2/folder2/_SUCCESS-123",
		"folder1/folder3/folder4/item3.parquet",
		"folder1/folder3/folder4/item3.parquet",
		"folder1/folder3/folder4/item4.parquet",
		"folder1/folder3/folder4/_SUCCESS-4544",
		"folder7/folder8/folder9/item5.parquet",
		"folder7/folder8/folder9/_SUCCESS-321",
	}
	currUpdate, filePrefix, err := findLatestVersion(files, 12)
	assert.NoError(t, err)
	assert.Equal(t, "folder1/folder3/folder4/", filePrefix)
	assert.Equal(t, uint64(4544), currUpdate)

	currUpdate, filePrefix, err = findLatestVersion(files, 4544)
	assert.NoError(t, err)
	assert.Equal(t, "", filePrefix)
	assert.Equal(t, uint64(0), currUpdate)

	currUpdate, filePrefix, err = findLatestVersion(files, 24234234)
	assert.NoError(t, err)
	assert.Equal(t, "", filePrefix)
	assert.Equal(t, uint64(0), currUpdate)
}
