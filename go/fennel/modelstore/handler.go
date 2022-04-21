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

type ModelStore struct {
	s3Bucket     string
	storeMutex   sync.Mutex
	endpointName string
	tierID       ftypes.RealmID
}

// NewModelStore creates a new ModelStore. There should not be more than one ModelStore.
func NewModelStore(args ModelStoreArgs, tierID ftypes.RealmID) *ModelStore {
	ms := ModelStore{
		s3Bucket:     args.ModelStoreS3Bucket,
		storeMutex:   sync.Mutex{},
		endpointName: args.ModelStoreEndpointName,
		tierID:       tierID,
	}
	return &ms
}

// Lock to avoid race condition when there are two models upload requests with room only for one more model
func (ms *ModelStore) Lock() {
	ms.storeMutex.Lock()
}

func (ms *ModelStore) Unlock() {
	ms.storeMutex.Unlock()
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
