package modelstore

import (
	"fmt"
	"sync"
)

type ModelStore struct {
	s3Bucket     string
	storeMutex   sync.Mutex
	endpointName string
}

// NewModelStore creates a new ModelStore. There should not be more than one ModelStore.
func NewModelStore(s3Bucket, endpointName string) *ModelStore {
	ms := ModelStore{
		s3Bucket:     s3Bucket,
		storeMutex:   sync.Mutex{},
		endpointName: endpointName,
	}
	return &ms
}

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
	return fmt.Sprintf("s3://%s/%s", ms.s3Bucket, fileName)
}
