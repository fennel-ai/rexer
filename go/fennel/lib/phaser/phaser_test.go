package phaser

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

func TestReadObjectss(t *testing.T) {
	readParquetFiles([]string{"asd"}, "ASd")
	assert.Equal(t, "asd", "asd1")
}

func readParquetFiles(filePaths []string, folder string) {

	for fileIndex, file := range filePaths {
		fmt.Println("File to read", file)

		fr, err := local.NewLocalFileReader("/Users/adityanambiar/Downloads/part-00000-2af002bd-ea51-4a15-b743-59092dfbd03a-c000.snappy.parquet")
		if err != nil {
			log.Fatal(err)
		}
		pr, err := reader.NewParquetReader(fr, new(ExampleItemScoreList), 4)
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
			ExampleItemScoreLists := make([]ExampleItemScoreList, 10)
			if i+10 < numRows {
				i += 10
			} else {
				i = numRows
			}

			if err = pr.Read(&ExampleItemScoreLists); err != nil {
				log.Println("Read error ::", err)
			}

			fmt.Println("key :", string(*ExampleItemScoreLists[0].Key))

			for _, ExampleItemScoreList := range ExampleItemScoreLists {
				v := value.NewList()
				for _, item := range ExampleItemScoreList.ItemScoreList {
					if item.ItemName != nil {
						v.Append(value.NewDict(map[string]value.Value{
							"item":  value.String(*item.ItemName),
							"score": value.Double(*item.Score),
						}))
					}
				}
				encodedString := base64.StdEncoding.EncodeToString(value.ToJSON(v))
				f.WriteString("SET " + string(*ExampleItemScoreList.Key) + " " + encodedString + "\n")
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
