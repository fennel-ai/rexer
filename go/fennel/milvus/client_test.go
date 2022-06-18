//go:build milvus

package milvus

import (
	"context"
	"fennel/engine/ast"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"time"
)

const TEST_TIER_ID = 123

func TestClient_CreateKNNIndex(t *testing.T) {
	milvusUrl := os.Getenv("MILVUS_URL")
	milvusClient, err := NewClient(MilvusArgs{Url: milvusUrl})
	assert.NoError(t, err)
	ctx := context.Background()
	defer cleanUpCollections(milvusClient, ctx)

	t0 := ftypes.Timestamp(0)
	agg := aggregate.Aggregate{
		Name:      "milvusAggTest",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "knn",
			Dim:             10,
			HyperParameters: "",
		},
		Id: 1,
	}
	err = milvusClient.CreateKNNIndex(ctx, agg, TEST_TIER_ID)
	assert.NoError(t, err)
	idx, err := milvusClient.client.DescribeIndex(ctx, getCollectionName(agg.Name, TEST_TIER_ID), VectorField)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(idx))
	assert.Equal(t, entity.IndexType("HNSW"), idx[0].IndexType())
	// Using default params
	assert.Equal(t, map[string]string{"index_type": "HNSW", "metric_type": "IP", "params": "{\"M\":\"32\",\"efConstruction\":\"128\"}"}, idx[0].Params())
	assert.NoError(t, milvusClient.DeleteCollection(ctx, agg.Name, TEST_TIER_ID))

	// Create an Annoy Index
	agg = aggregate.Aggregate{
		Name:      "milvusAggTestAnnoy",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "knn",
			Dim:             10,
			HyperParameters: "{\"index\": \"annoy\", \"metric\":\"l2\", \"nTrees\":23}",
		},
		Id: 1,
	}
	err = milvusClient.CreateKNNIndex(ctx, agg, TEST_TIER_ID)
	assert.NoError(t, err)
	idx, err = milvusClient.client.DescribeIndex(ctx, getCollectionName(agg.Name, TEST_TIER_ID), VectorField)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(idx))
	assert.Equal(t, entity.IndexType("ANNOY"), idx[0].IndexType())
	// Using default params
	assert.Equal(t, map[string]string{"index_type": "ANNOY", "metric_type": "L2", "params": "{\"n_trees\":\"23\"}"}, idx[0].Params())
	assert.NoError(t, milvusClient.DeleteCollection(ctx, agg.Name, TEST_TIER_ID))

	// Create a flat index
	agg = aggregate.Aggregate{
		Name:      "milvusAggTestFlat",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "knn",
			Dim:             10,
			HyperParameters: "{\"index\": \"flat\", \"metric\":\"l2\"}",
		},
		Id: 1,
	}
	err = milvusClient.CreateKNNIndex(ctx, agg, TEST_TIER_ID)
	assert.NoError(t, err)
	idx, err = milvusClient.client.DescribeIndex(ctx, getCollectionName(agg.Name, TEST_TIER_ID), VectorField)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(idx))
	assert.Equal(t, entity.IndexType("FLAT"), idx[0].IndexType())
	// Using default params
	assert.Equal(t, map[string]string{"index_type": "FLAT", "metric_type": "L2", "params": "{\"nlist\":\"1024\"}"}, idx[0].Params())
	assert.NoError(t, milvusClient.DeleteCollection(ctx, agg.Name, TEST_TIER_ID))
}

func TestClient_InsertStream_GetNeighbors(t *testing.T) {
	milvusUrl := os.Getenv("MILVUS_URL")
	milvusClient, err := NewClient(MilvusArgs{Url: milvusUrl})
	ctx := context.Background()
	defer cleanUpCollections(milvusClient, ctx)

	t0 := ftypes.Timestamp(0)
	agg := aggregate.Aggregate{
		Name:      "milvusAggTestGetEmbedding",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "knn",
			Dim:             10,
			HyperParameters: "{\"index\": \"flat\", \"metric\":\"l2\"}",
		},
		Id: 1,
	}
	err = milvusClient.CreateKNNIndex(ctx, agg, TEST_TIER_ID)
	assert.NoError(t, err)
	table := value.NewList()
	table.Grow(100)
	for i := 0; i < 100; i++ {
		vector := make([]float32, 10)
		for j := 0; j < 10; j++ {
			vector[j] = float32(i)
		}
		d := value.NewDict(map[string]value.Value{"groupkey": FromList(vector), "value": value.Int(i), "timestamp": value.Int(t0)})
		table.Append(d)
	}
	err = milvusClient.InsertStream(ctx, agg, table, TEST_TIER_ID)
	assert.NoError(t, err)
	time.Sleep(time.Second)
	// Get neighbors
	vec := make([]float32, 10)
	vec2 := make([]float32, 10)
	for i := 0; i < 10; i++ {
		vec[i] = float32(49.2)
		vec2[i] = float32(59.8)
	}
	res, err := milvusClient.GetNeighbors(ctx, agg, []value.Value{FromList(vec), FromList(vec2)}, value.NewDict(map[string]value.Value{"topK": value.Int(2)}), TEST_TIER_ID)
	assert.NoError(t, err)
	expected := []value.Value{
		value.NewList(
			value.NewDict(map[string]value.Value{PrimaryField: value.Int(49), ScoreField: value.Double(0.400003045797348)}),
			value.NewDict(map[string]value.Value{PrimaryField: value.Int(50), ScoreField: value.Double(6.399988174438477)}),
		),
		value.NewList(
			value.NewDict(map[string]value.Value{PrimaryField: value.Int(60), ScoreField: value.Double(0.400003045797348)}),
			value.NewDict(map[string]value.Value{PrimaryField: value.Int(59), ScoreField: value.Double(6.399988174438477)}),
		),
	}

	for i, v := range res {
		assert.Equal(t, expected[i], v)
	}
}

func TestClient_InsertStream_GetEmbedding(t *testing.T) {
	milvusUrl := os.Getenv("MILVUS_URL")
	milvusClient, err := NewClient(MilvusArgs{Url: milvusUrl})
	ctx := context.Background()
	defer cleanUpCollections(milvusClient, ctx)

	t0 := ftypes.Timestamp(0)
	agg := aggregate.Aggregate{
		Name:      "milvusAggTestGetEmbedding",
		Query:     ast.MakeInt(0),
		Timestamp: t0,
		Options: aggregate.Options{
			AggType:         "knn",
			Dim:             10,
			HyperParameters: "",
		},
		Id: 1,
	}
	err = milvusClient.CreateKNNIndex(ctx, agg, TEST_TIER_ID)
	assert.NoError(t, err)
	table := value.NewList()
	table.Grow(100)
	for i := 0; i < 100; i++ {
		vector := make([]float32, 10)
		for j := 0; j < 10; j++ {
			vector[j] = float32(i)
		}
		d := value.NewDict(map[string]value.Value{"value": value.Int(i), "timestamp": value.Int(t0), "groupkey": FromList(vector)})
		table.Append(d)
	}
	err = milvusClient.InsertStream(ctx, agg, table, TEST_TIER_ID)
	assert.NoError(t, err)
	time.Sleep(time.Second)

	result, err := milvusClient.GetEmbedding(ctx, agg, value.NewList([]value.Value{value.Int(0), value.Int(1), value.Int(2)}...), TEST_TIER_ID)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(result))
	for i := 0; i < 3; i++ {
		vec := make([]float32, 10)
		for j := 0; j < 10; j++ {
			vec[j] = float32(i)
		}
		assert.Equal(t, FromList(vec), result[i])
	}
}

func cleanUpCollections(milvusClient Client, ctx context.Context) {
	collections, err := milvusClient.client.ListCollections(ctx)
	if err != nil {
		return
	}
	for _, collection := range collections {
		if strings.HasPrefix(collection.Name, "t_"+fmt.Sprint(TEST_TIER_ID)+"$milvusAggTest") {
			milvusClient.client.DropCollection(ctx, collection.Name)
		}
	}
}
