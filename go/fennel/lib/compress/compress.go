package compress

import (
	"encoding/json"
	"fmt"

	"github.com/golang/snappy"
)

func Encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, b), nil
}

func Decode(b []byte, v any) error {
	b, err := snappy.Decode(nil, b)
	if err != nil {
		return fmt.Errorf("failed to decompress: %s", err)
	}
	return json.Unmarshal(b, v)
}
