package milvus

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	hp "fennel/lib/hyperparam"
	"fennel/lib/value"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
)

type MilvusArgs struct {
	Url string `arg:"--milvus-url,env:MILVUS_URL,help:Milvus url"`
}

type Client struct {
	client client.Client
}

const (
	PrimaryField = `item`
	VectorField  = `vector`
	ScoreField   = `score`
)

var supportedHyperParameters = hp.HyperParamRegistry{
	"knn": map[string]hp.HyperParameterInfo{
		"metric":         {Default: "ip", Type: reflect.String, Options: []string{"ip", "l2", "hamming", "jaccard"}},
		"index":          {Default: "hnsw", Type: reflect.String, Options: []string{"flat", "ivf_flat", "hnsw", "annoy"}},
		"nList":          {Default: 1024, Type: reflect.Int, Options: nil},
		"M":              {Default: 32, Type: reflect.Int, Options: nil},
		"efConstruction": {Default: 128, Type: reflect.Int, Options: nil},
		// larger number of trees produces accurate results but creates larger indexes
		"nTrees": 		  {Default: 32, Type: reflect.Int, Options: nil},
	},
}

var knnIndexSearchParams = hp.HyperParamRegistry{
	"flat": map[string]hp.HyperParameterInfo{
		"nprobe": {Default: 12, Type: reflect.Int, Options: nil},
	},
	"ivf_flat": map[string]hp.HyperParameterInfo{
		"nprobe": {Default: 12, Type: reflect.Int, Options: nil},
	},
	"hnsw": map[string]hp.HyperParameterInfo{
		"ef": {Default: 128, Type: reflect.Int, Options: nil},
	},
	"annoy": map[string]hp.HyperParameterInfo{
		"searchK": {Default: -1, Type: reflect.Int, Options: nil},
	},
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
// Public API
//================================================

func (c Client) Close() error {
	return c.client.Close()
}

func getCollectionName(aggName ftypes.AggName, tierId ftypes.RealmID) string {
	// Milvus collection name does not support ':'
	return "t_" + fmt.Sprint(tierId) + "$" + string(aggName)
}

func (c Client) CreateKNNIndex(ctx context.Context, agg aggregate.Aggregate, tierId ftypes.RealmID) error {
	// get fields from the aggregate and set them in schema
	schema := &entity.Schema{
		CollectionName: getCollectionName(agg.Name, tierId),
		Description:    fmt.Sprintf("Collection for agg %s", agg.Name),
		Fields: []*entity.Field{
			{
				Name:       PrimaryField,
				DataType:   entity.FieldTypeInt64,
				PrimaryKey: true,
				AutoID:     false,
			},
			{
				Name:       "Timestamp",
				DataType:   entity.FieldTypeInt64,
				PrimaryKey: false,
				AutoID:     false,
			},
			{
				Name:     VectorField,
				DataType: entity.FieldTypeFloatVector,
				TypeParams: map[string]string{
					"dim": fmt.Sprint(agg.Options.Dim),
				},
			},
		},
	}

	err := c.client.CreateCollection(
		ctx, // ctx
		schema,
		2, // shardNum
	)
	if err != nil {
		return err
	}
	hyperparameters, err := hp.GetHyperParameters("knn", agg.Options.HyperParameters, supportedHyperParameters)
	if err != nil {
		return err
	}
	metric, err := getMetric(hyperparameters["metric"].(string))
	if err != nil {
		return err
	}

	idx, err := getIndex(hyperparameters, metric)
	if err != nil {
		return err
	}
	err = c.client.CreateIndex(
		ctx,                                 // ctx
		getCollectionName(agg.Name, tierId), // collectionName
		VectorField,                         // fieldName
		idx,                                 // index
		false,                               // async
	)
	if err != nil {
		return err
	}
	return c.client.LoadCollection(
		ctx,
		getCollectionName(agg.Name, tierId), // ctx
		false,                               // async
	)
}

func (c Client) InsertStream(ctx context.Context, agg aggregate.Aggregate, table value.List, tierId ftypes.RealmID) error {
	// if the transformed profiles are empty, nothing to write to milvus
	if table.Len() == 0 {
		return nil
	}
	// Change this to string once Milvus 2.1 is released.
	ids := make([]int64, 0, table.Len())
	timestamps := make([]int64, 0, table.Len())
	vectors := make([][]float32, 0, table.Len())

	for i := 0; i < table.Len(); i++ {
		rowVal, _ := table.At(i)
		row, ok := rowVal.(value.Dict)
		if !ok {
			return fmt.Errorf("%s expected to be dict but found: '%v'", agg.Source, rowVal)
		}
		id, ok := row.Get("value")
		if !ok {
			return fmt.Errorf("%s '%v' does not have a field called 'groupkey'", agg.Source, rowVal)
		}
		ts, ok := row.Get("timestamp")
		if !ok || value.Types.Int.Validate(ts) != nil {
			return fmt.Errorf("action '%v' does not have a field called 'timestamp' with datatype of 'int'", row)
		}
		v, ok := row.Get("groupkey")
		if !ok {
			return fmt.Errorf("action '%v' does not have a field called 'value'", row)
		}
		ts_int := ts.(value.Int)
		key := id.(value.Int)
		ids = append(ids, int64(key))
		timestamps = append(timestamps, int64(ts_int))
		vector, err := toList(v.(value.List))
		if err != nil {
			return err
		}
		vectors = append(vectors, vector)
	}

	idColumn := entity.NewColumnInt64(PrimaryField, ids)
	timestampColumn := entity.NewColumnInt64("Timestamp", timestamps)
	vectorColumn := entity.NewColumnFloatVector(VectorField, int(agg.Options.Dim), vectors)
	_, err := c.client.Insert(
		ctx,                                 // ctx
		getCollectionName(agg.Name, tierId), // collectionName
		"",                                  // partitionName
		idColumn,                            // columnarData
		timestampColumn,                     // columnarData
		vectorColumn,                        // columnarData
	)
	return err
}

func (c Client) GetNeighbors(ctx context.Context, agg aggregate.Aggregate, vectors []value.Value, kwarg value.Dict, tierId ftypes.RealmID) ([]value.Value, error) {
	hyperparameters, err := hp.GetHyperParameters("knn", agg.Options.HyperParameters, supportedHyperParameters)

	if err != nil {
		return nil, err
	}
	indexType := hyperparameters["index"].(string)

	var inputSp value.Dict
	if inpParams, ok := kwarg.Get("searchParams"); !ok {
		inputSp = value.Dict{}
	} else {
		inputSp = inpParams.(value.Dict)
	}

	searchParams, err := hp.GetHyperParametersFromMap(indexType, inputSp, knnIndexSearchParams)
	if err != nil {
		return nil, err
	}

	sp, err := getSearchParams(indexType, searchParams)
	if err != nil {
		return nil, err
	}

	metric, err := getMetric(hyperparameters["metric"].(string))
	if err != nil {
		return nil, err
	}

	var topK int

	if tmp, ok := kwarg.Get("topK"); !ok {
		return nil, fmt.Errorf("Expected topK to be passed as kwarg")
	} else {
		t, err := getDouble(tmp)
		if err != nil {
			return nil, err
		}
		topK = int(t)
	}

	milvusVectors := make([]entity.Vector, len(vectors))

	for i := 0; i < len(vectors); i++ {
		vector, err := toList(vectors[i].(value.List))
		if err != nil {
			return nil, err
		}
		milvusVectors[i] = entity.FloatVector(vector)
	}

	searchResult, err := c.client.Search(
		ctx,                                 // ctx
		getCollectionName(agg.Name, tierId), // collectionName
		[]string{},                          // partitionNames
		"",                                  // expr
		[]string{PrimaryField},              // outputFields
		milvusVectors,                       // vectors
		VectorField,                         // vectorField
		metric,                              // metricType
		topK,                                // topK
		sp,                                  // sp
	)

	if err != nil {
		log.Fatal("fail to search collection:", err.Error())
	}

	allResults := make([]value.Value, len(searchResult))
	for sInd, result := range searchResult {
		var idColumn *entity.ColumnInt64
		var knnResult value.List
		knnResult.Grow(result.IDs.Len())

		for _, field := range result.Fields {
			if field.Name() == PrimaryField {
				c, ok := field.(*entity.ColumnInt64)
				if ok {
					idColumn = c
				}
			}
		}
		if idColumn == nil {
			return nil, fmt.Errorf("Expected to find ID column")
		}
		for i := 0; i < result.ResultCount; i++ {
			id, err := idColumn.ValueByIdx(i)
			if err != nil {
				return nil, err
			}
			knnResult.Append(value.NewDict(map[string]value.Value{PrimaryField: value.Int(id), ScoreField: value.Double(result.Scores[i])}))
		}
		allResults[sInd] = knnResult
	}
	return allResults, nil
}

func (c Client) GetEmbedding(ctx context.Context, agg aggregate.Aggregate, keys value.List, tierId ftypes.RealmID) ([]value.Value, error) {
	ids := make([]int64, keys.Len())
	for i := 0; i < keys.Len(); i++ {
		idVal, _ := keys.At(i)
		id, err := getDouble(idVal)
		if err != nil {
			return nil, err
		}
		ids[i] = int64(id)
	}
	idColumn := entity.NewColumnInt64(PrimaryField, ids)

	queryResult, err := c.client.QueryByPks(
		ctx,                                 // ctx
		getCollectionName(agg.Name, tierId), // collectionName
		[]string{},                          // partitionNames
		idColumn,                            // expr
		[]string{VectorField},               // outputFields
	)

	if err != nil {
		return nil, err
	}

	if len(queryResult) == 0 {
		return []value.Value{}, nil
	}

	allResults := make([]value.Value, queryResult[0].Len())
	for _, field := range queryResult {
		if field.Name() == VectorField {
			if c, ok := field.(*entity.ColumnFloatVector); !ok {
				return nil, fmt.Errorf("embeddings returned are not of type float vector")
			} else {
				floatVectors := c.Data()
				for i := 0; i < len(floatVectors); i++ {
					allResults[i] = FromList(floatVectors[i])
				}
			}
			break
		}
	}

	return allResults, nil
}

func (c Client) DeleteCollection(ctx context.Context, aggName ftypes.AggName, tierId ftypes.RealmID) error {
	return c.client.DropCollection(ctx, getCollectionName(aggName, tierId))
}

//================================================
// Private helpers/interface
//================================================

func getIndex(hyperparameters map[string]interface{}, metric entity.MetricType) (entity.Index, error) {
	switch hyperparameters["index"] {
	case "flat":
		return entity.NewIndexFlat(
			metric,
			hyperparameters["nList"].(int),
		)
	case "ivf_flat":
		return entity.NewIndexFlat(
			metric,
			hyperparameters["nList"].(int),
		)
	case "hnsw":
		return entity.NewIndexHNSW(
			metric,
			hyperparameters["M"].(int),
			hyperparameters["efConstruction"].(int),
		)
	case "annoy":
		return entity.NewIndexANNOY(
			metric,
			hyperparameters["nTrees"].(int),
		)
	default:
		return nil, fmt.Errorf("unsupported index %s", hyperparameters["index"])
	}
}

func getSearchParams(indexType string, searchParameters map[string]interface{}) (entity.SearchParam, error) {
	switch indexType {
	case "flat":
		return entity.NewIndexFlatSearchParam(
			searchParameters["nprobe"].(int),
		)
	case "ivf_flat":
		return entity.NewIndexIvfFlatSearchParam(
			searchParameters["nprobe"].(int),
		)
	case "hnsw":
		return entity.NewIndexHNSWSearchParam(
			searchParameters["ef"].(int),
		)
	case "annoy":
		return entity.NewIndexANNOYSearchParam(
			searchParameters["searchK"].(int),
		)
	default:
		return nil, fmt.Errorf("unsupported index %s", indexType)
	}
}

func getMetric(metric string) (entity.MetricType, error) {
	switch metric {
	case "ip":
		return entity.IP, nil
	case "l2":
		return entity.L2, nil
	case "hamming":
		return entity.HAMMING, nil
	case "jaccard":
		return entity.JACCARD, nil
	default:
		return entity.L2, fmt.Errorf("unsupported metric %s", metric)
	}
}

func getDouble(v value.Value) (float32, error) {
	if d, ok := v.(value.Double); ok {
		return float32(d), nil
	}

	if i, ok := v.(value.Int); ok {
		return float32(i), nil
	}
	return 0, fmt.Errorf("value [%s] is not a $$ number", v.String())
}

func toList(l value.List) ([]float32, error) {
	var err error
	ret := make([]float32, l.Len())
	for i := 0; i < l.Len(); i++ {
		v, _ := l.At(i)
		ret[i], err = getDouble(v)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func FromList(l []float32) value.List {
	v := value.NewList()
	for _, x := range l {
		v.Append(value.Double(x))
	}
	return v
}
