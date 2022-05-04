//go:build sagemaker

package s3

import (
	"encoding/base64"
	"fennel/lib/value"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func TestClient(t *testing.T) {
	c := NewClient(S3Args{Region: "ap-south-1"})
	contents := "some random text"
	file := strings.NewReader(contents)
	fileName := "some_file.txt"
	bucketName := "my-xgboost-test-bucket-2"

	err := c.Upload(file, fileName, bucketName)
	assert.NoError(t, err)

	found, err := c.Download(fileName, bucketName)
	assert.NoError(t, err)
	assert.Equal(t, string(found), contents)

	err = c.Delete(fileName, bucketName)
	assert.NoError(t, err)
}

type ItemScore struct {
	ItemName *string  `parquet:"name=item, type=BYTE_ARRAY, convertedtype=UTF8"`
	Score    *float64 `parquet:"name=score, type=FLOAT"`
}

type Example struct {
	Key      *string     `parquet:"name=groupkey, type=BYTE_ARRAY, convertedtype=UTF8"`
	ItemName []ItemScore `parquet:"name=item_list, type=LIST"`
}

func TestListObjectsClient(t *testing.T) {
	c := NewClient(S3Args{Region: "us-west-2"})
	bucketName := "p-2-offline-aggregate-output"
	x, err := c.ListFiles(bucketName, "t-106-offline-aggregate-output/t_106/movie_topk_tags_4-604800")
	//x, err := c.ListFiles("p-2-offline-aggregate-output", "")

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("-------------")
	var filesToDownload []string
	var prefixToUpdate, updateVersion string
	for _, y := range x {
		fmt.Println(y)
		pathArray := strings.Split(y, "/")
		if len(pathArray) > 0 && strings.HasPrefix(pathArray[len(pathArray)-1], "_SUCCESS-") {
			fmt.Println("FOUND SUCCESS")
			updateVersion = strings.Replace(pathArray[len(pathArray)-1], "_SUCCESS-", "", 1)
			fmt.Println(y, "::", updateVersion)
			prefixToUpdate = strings.Join(pathArray[:len(pathArray)-1], "/")
			fmt.Println("Prefix to Update to Redis", "::", prefixToUpdate)
		}
	}

	if updateVersion != "" {
		for _, y := range x {
			fmt.Println(y)
			if strings.HasPrefix(y, prefixToUpdate) && !strings.HasSuffix(y, fmt.Sprintf("_SUCCESS-%s", updateVersion)) {
				filesToDownload = append(filesToDownload, y)
			}
		}
	}
	fmt.Println("==========================================================")
	fmt.Println("Files to Download")
	for _, y := range filesToDownload {
		fmt.Println(y)
	}

	folder := "/tmp"
	err = c.BatchDiskDownload(filesToDownload, bucketName, folder)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Done downloading")

	for _, file := range filesToDownload {
		fmt.Println("File to read", file)
		pathArray := strings.Split(file, "/")
		fr, err := local.NewLocalFileReader(fmt.Sprintf("%s/%s", folder, pathArray[len(pathArray)-1]))
		if err != nil {
			log.Fatal(err)
		}

		pr, err := reader.NewParquetReader(fr, new(Example), 4)
		if err != nil {
			log.Fatal(err)
		}
		numRows := int(pr.GetNumRows())
		for i := 0; i < numRows/10; i++ {
			if i%2 == 0 {
				pr.SkipRows(10) //skip 10 rows
				continue
			}
			examples := make([]Example, 10) //read 10 rows
			if err = pr.Read(&examples); err != nil {
				log.Println("Read error", err)
			}
			fmt.Println(examples)
		}

		pr.ReadStop()
		fr.Close()
	}
}

func TestReadObjectss(t *testing.T) {
	readParquetFiles([]string{"asd"}, "ASd")
	assert.Equal(t, "asd", "asd1")
}

func readParquetFiles(filePaths []string, folder string) {

	for fileIndex, file := range filePaths {
		fmt.Println("File to read", file)
		// pathArray := strings.Split(file, "/")
		// fr, err := local.NewLocalFileReader(fmt.Sprintf("%s/%s", folder, pathArray[len(pathArray)-1]))
		// if err != nil {
		// 	log.Fatal(err)
		// }
		fr, err := local.NewLocalFileReader("/Users/adityanambiar/Downloads/part-00000-2af002bd-ea51-4a15-b743-59092dfbd03a-c000.snappy.parquet")
		if err != nil {
			log.Fatal(err)
		}
		pr, err := reader.NewParquetReader(fr, new(Example), 4)
		if err != nil {
			fmt.Println("Error", err)
			log.Fatal(err)
		}
		numRows := int(pr.GetNumRows())

		fmt.Println("Number of rows", numRows)

		f, err := os.Create("/tmp/" + fmt.Sprintf("%d", fileIndex) + ".txt")
		if err != nil {
			log.Fatal(err)
		}

		for i := 0; i < numRows; i++ {
			examples := make([]Example, 10)
			if i+10 < numRows {
				i += 10
			} else {
				i = numRows
			}

			if err = pr.Read(&examples); err != nil {
				log.Println("Read error ::", err)
			}

			fmt.Println("key :", string(*examples[0].Key))

			for _, example := range examples {
				v := value.NewList()
				for _, item := range example.ItemName {
					if item.ItemName != nil {
						v.Append(value.NewDict(map[string]value.Value{
							"item":  value.String(*item.ItemName),
							"score": value.Double(*item.Score),
						}))
					}
				}
				encodedString := base64.StdEncoding.EncodeToString(value.ToJSON(v))
				f.WriteString("SET " + string(*example.Key) + " " + encodedString + "\n")
			}
		}
		f.Close()
		pr.ReadStop()
		fr.Close()
		cmd := "cat /tmp/" + fmt.Sprintf("%d", fileIndex) + ".txt" + " | redis-cli --pipe"
		out, err := exec.Command("bash", "-c", cmd).Output()
		fmt.Println(string(out))

		if strings.Contains(string(out), "errors: 0, replies: "+fmt.Sprintf("%d", numRows)) {
			fmt.Println("Success")
		} else {
			fmt.Println("Failed")
		}

		if err != nil {
			fmt.Println(err)
			log.Fatal(err)
		}
		// err = os.Remove("GeeksforGeeks.txt")
		// if err != nil {
		// 	log.Fatal(err)
		// }
	}
}
