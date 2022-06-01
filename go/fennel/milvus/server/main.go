package main

import (
	"fmt"
	"log"
	"math/rand"

	"context"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

var MILVUS_URL = "a30b44469bb91442ca880b1611e00ab5-1934733413.us-west-2.elb.amazonaws.com:19530"

func main() {
	fmt.Println("Hello World!")

	milvusClient, err := client.NewGrpcClient(
		context.Background(), // ctx
		MILVUS_URL,           // addr
	)
	if err != nil {
		log.Fatal("failed to connect to Milvus:", err.Error())
	}
	defer milvusClient.Close()

	var (
		collectionName = "book2"
	)
	schema := &entity.Schema{
		CollectionName: collectionName,
		Description:    "Test book search",
		Fields: []*entity.Field{
			{
				Name:       "book_id",
				DataType:   entity.FieldTypeInt64,
				PrimaryKey: true,
				AutoID:     false,
			},
			{
				Name:       "word_count",
				DataType:   entity.FieldTypeInt64,
				PrimaryKey: false,
				AutoID:     false,
			},
			{
				Name:     "book_intro",
				DataType: entity.FieldTypeFloatVector,
				TypeParams: map[string]string{
					"dim": "2",
				},
			},
		},
	}

	err = milvusClient.CreateCollection(
		context.Background(), // ctx
		schema,
		2, // shardNum
	)
	if err != nil {
		log.Fatal("failed to create collection:", err.Error())
	}

	bookIDs := make([]int64, 0, 2000)
	wordCounts := make([]int64, 0, 2000)
	bookIntros := make([][]float32, 0, 2000)
	for i := 0; i < 2000; i++ {
		bookIDs = append(bookIDs, int64(i))
		wordCounts = append(wordCounts, int64(i+10000))
		v := make([]float32, 0, 2)
		for j := 0; j < 2; j++ {
			v = append(v, rand.Float32())
		}
		bookIntros = append(bookIntros, v)
	}
	idColumn := entity.NewColumnInt64("book_id", bookIDs)
	wordColumn := entity.NewColumnInt64("word_count", wordCounts)
	introColumn := entity.NewColumnFloatVector("book_intro", 2, bookIntros)

	_, err = milvusClient.Insert(
		context.Background(), // ctx
		"book2",              // CollectionName
		"",                   // partitionName
		idColumn,             // columnarData
		wordColumn,           // columnarData
		introColumn,          // columnarData
	)
	if err != nil {
		log.Fatal("failed to insert data:", err.Error())
	}
	idx, err := entity.NewIndexIvfFlat( // NewIndex func
		entity.L2, // metricType
		1024,      // ConstructParams
	)
	if err != nil {
		log.Fatal("fail to create ivf flat index parameter:", err.Error())
	}
	err = milvusClient.CreateIndex(
		context.Background(), // ctx
		"book2",              // CollectionName
		"book_intro",         // fieldName
		idx,                  // entity.Index
		false,                // async
	)
	if err != nil {
		log.Fatal("fail to create index:", err.Error())
	}
	err = milvusClient.LoadCollection(
		context.Background(), // ctx
		"book2",              // CollectionName
		false,                // async
	)
	if err != nil {
		log.Fatal("failed to load collection:", err.Error())
	}

	sp, _ := entity.NewIndexFlatSearchParam( // NewIndex*SearchParam func
		10, // searchParam
	)
	searchResult, err := milvusClient.Search(
		context.Background(), // ctx
		"book2",              // CollectionName
		[]string{},           // partitionNames
		"",                   // expr
		[]string{"book_id"},  // outputFields
		[]entity.Vector{entity.FloatVector([]float32{0.1, 0.2})}, // vectors
		"book_intro", // vectorField
		entity.L2,    // metricType
		20,           // topK
		sp,           // sp
	)
	if err != nil {
		log.Fatal("fail to search collection:", err.Error())
	}
	fmt.Println("Query by PK")
	result, err := milvusClient.QueryByPks(context.Background(), "book", []string{}, entity.NewColumnInt64("book_id", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), []string{"book_id", "word_count", "book_intro"})

	if err != nil {
		log.Fatal("fail to query by pks:", err.Error())
	}
	fmt.Println(result)
	// Print ids of the result
	colInt64, _ := result[0].(*entity.ColumnInt64)

	for res, v := range colInt64.Data() {
		fmt.Println(res, v)
	}
	colInt64, _ = result[1].(*entity.ColumnInt64)

	for res, v := range colInt64.Data() {
		fmt.Println(res, v)
	}
	fmt.Println("Query vectors")
	vectorData, ok := result[2].(*entity.ColumnFloatVector)
	if !ok {
		log.Fatal("fail to get vector data")
	}
	for res, v := range vectorData.Data() {
		fmt.Println(res, v)
	}

	fmt.Println("-----------------------------------------------------")
	fmt.Printf("%#v\n", searchResult)
	for _, sr := range searchResult {
		fmt.Println(sr.IDs)
		fmt.Println(sr.Scores)
	}
	err = milvusClient.ReleaseCollection(
		context.Background(), // ctx
		"book2",              // CollectionName
	)
	if err != nil {
		log.Fatal("failed to release collection:", err.Error())
	}
}
