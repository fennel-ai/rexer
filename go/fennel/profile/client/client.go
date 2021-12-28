package client

import (
	"bytes"
	"encoding/json"
	"fennel/profile/lib"
	"fennel/value"
	"fmt"
	"io/ioutil"
	"net/http"
)

type Client struct {
	url string
}

func NewClient(url string) Client {
	return Client{url}
}

// Get if no matching value is found, a nil pointer is returned with no error
// If a matching value is found, a valid Value pointer is returned with no error
// If an error occurs, a nil pointer is returned with a non-nil error
func (c Client) Get(otype lib.OType, oid uint64, key string, version uint64) (*value.Value, error) {
	postBody, err := json.Marshal(map[string]interface{}{
		"OType":   otype,
		"Oid":     oid,
		"Key":     key,
		"Version": version,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal error on client: %v", err)
	}
	reqBody := bytes.NewBuffer(postBody)
	// TODO: should these be in body (which means POST) or in headers with GET method?
	response, err := http.Post(c.url+"/get", "application/json", reqBody)
	if err != nil {
		return nil, fmt.Errorf("server error: %v", err)
	}
	//Read the response body
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		// i.e. no valid value is found, so we return nil pointer
		return nil, nil
	}
	v, err := value.UnmarshalJSON(body)
	if err != nil {
		return nil, err
	} else {
		return &v, nil
	}
}

func (c Client) Set(otype lib.OType, oid uint64, key string, version uint64, v value.Value) error {
	vser, err := v.MarshalJSON()
	if err != nil {
		return fmt.Errorf("could not marshal value: %v", err)
	}
	item := lib.ProfileItemSer{Otype: otype, Oid: oid, Key: key, Version: version, Value: vser}
	postBody, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal error on client: %v", err)
	}

	reqBody := bytes.NewBuffer(postBody)
	response, err := http.Post(c.url+"/set", "application/json", reqBody)
	if err != nil {
		return fmt.Errorf("server error: %v", err)
	}
	// verify server sent no error
	defer response.Body.Close()
	_, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	return nil
}
