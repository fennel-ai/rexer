package milvus

import (
	"context"
	"fennel/lib/aggregate"
	"fennel/lib/value"
	"fmt"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

type MilvusArgs struct {
	Url string `arg:"--milvus-url,env:MILVUS_URL,help:Milvus url"`
}

type Client struct {
	client client.Client
}

func NewClient(args MilvusArgs) (Client, error) {
	client, err := client.NewGrpcClient(
		context.Background(), // ctx
		args.Url,             // addr
	)
	return Client{
		client: client,
	}, err
}

//================================================
// Public API for Phaser
//================================================

func (c Client) Close() error {
	return c.client.Close()
}

func (c Client) CreateKNNIndex(agg aggregate.Aggregate) error {
	// get fields from the aggregate and set them in schema
	schema := &entity.Schema{
		CollectionName: string(agg.Name),
		Description:    fmt.Sprintf("Collection for agg %s", agg.Name),
		Fields: []*entity.Field{
			{
				Name:       "id",
				DataType:   entity.FieldTypeString,
				PrimaryKey: true,
				AutoID:     false,
			},
			{
				Name:     "vector",
				DataType: entity.FieldTypeFloatVector,
				TypeParams: map[string]string{
					"dim": string(agg.Options.Dim),
				},
			},
		},
	}

	err := c.client.CreateCollection(
		context.Background(), // ctx
		schema,
		2, // shardNum
	)
	if err != nil {
		return err
	}

	idx, err := entity.NewIndexIvfFlat( // NewIndex func
		entity.L2, // metricType
		1024,      // ConstructParams
	)

	if err != nil {
		return err
	}

	return c.client.CreateIndex(
		context.Background(), // ctx
		string(agg.Name),     // CollectionName
		"vector",             // fieldName
		idx,                  // entity.Index
		false,                // async
	)
}

func (c Client) InsertStream(index Index, stream Stream) error {
	for {
		v, err := stream.Next()
		if err != nil {
			return err
		}
		if v == nil {
			break
		}
		if err := index.Insert(v); err != nil {
			return err
		}
	}
	return nil
}

func (c Client) GetNeighbors(index Index, id string, topK int) (value.List, error) {
	return index.GetNeighbors(id, topK)
}

func (c Client) GetEmbedding(index Index, id string) (value.Value, error) {
	return index.Get(id)
}

//================================================
// Private helpers/interface
//================================================
