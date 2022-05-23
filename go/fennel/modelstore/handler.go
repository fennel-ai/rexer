package modelstore

import (
	"fmt"
	"sync"

	"fennel/lib/ftypes"
)

type ModelStoreArgs struct {
	ModelStoreS3Bucket     string `arg:"--model-store-s3-bucket,env:MODEL_STORE_S3_BUCKET,help:Model Store S3 bucket name"`
	ModelStoreEndpointName string `arg:"--model-store-endpoint,env:MODEL_STORE_ENDPOINT,help:Model Store endpoint name"`
}

// Model Stores are kept seperate from Pretrained model stores. This is because
// the pretrained models have shared S3 artifacts across all planes and have one endpoint per model ( check model_store.go ),
// rather than all models sharing an endpoint ( Harder since these models are more heavy).
// Pretrained models use serverless config for now ( can be changed later)
type PreTrainedModelStore struct {
	endpointName string
	model        string
}

type ModelStore struct {
	s3Bucket     string
	endpointName string
	tierID       ftypes.RealmID
	// Mutex to avoid race condition when there are two models upload requests with room only for one more model
	sync.Mutex
}

// NewModelStore creates a new ModelStore. There should not be more than one ModelStore.
func NewModelStore(args ModelStoreArgs, tierID ftypes.RealmID) *ModelStore {
	ms := ModelStore{
		s3Bucket:     args.ModelStoreS3Bucket,
		Mutex:        sync.Mutex{},
		endpointName: args.ModelStoreEndpointName,
		tierID:       tierID,
	}
	return &ms
}

// TestSetEndpointName should only be used in tests
func (ms *ModelStore) TestSetEndpointName(endpointName string) {
	ms.endpointName = endpointName
}

func (ms *ModelStore) EndpointName() string {
	return ms.endpointName
}

func (ms *ModelStore) S3Bucket() string {
	return ms.s3Bucket
}

func (ms *ModelStore) GetArtifactPath(fileName string) string {
	return fmt.Sprintf("s3://%s/t-%d/%s", ms.s3Bucket, ms.tierID, fileName)
}
