package client

import (
	"bytes"
	"encoding/json"
	"fennel/data/lib"
	"fennel/value"
	"fmt"
	"io/ioutil"
	"net/http"

	"google.golang.org/protobuf/proto"
)

type Client struct {
	url string
}

func NewClient(url string) Client {
	return Client{url}
}

func (c Client) logURL() string {
	return fmt.Sprintf("%s:%d/log", c.url, lib.PORT)
}

func (c Client) fetchURL() string {
	return fmt.Sprintf("%s:%d/fetch", c.url, lib.PORT)
}

func (c Client) countURL() string {
	return fmt.Sprintf("%s:%d/count", c.url, lib.PORT)
}
func (c Client) rateURL() string {
	return fmt.Sprintf("%s:%d/rate", c.url, lib.PORT)
}

// GetProfile if no matching value is found, a nil pointer is returned with no error
// If a matching value is found, a valid Value pointer is returned with no error
// If an error occurs, a nil pointer is returned with a non-nil error
func (c *Client) GetProfile(otype lib.OType, oid uint64, key string, version uint64) (*value.Value, error) {
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

func (c *Client) SetProfile(otype lib.OType, oid uint64, key string, version uint64, v value.Value) error {
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

func (c *Client) FetchActions(request lib.ActionFetchRequest) ([]lib.Action, error) {
	protoRequest := lib.ToProtoActionFetchRequest(request)
	ser, err := proto.Marshal(&protoRequest)
	if err != nil {
		return nil, err
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(c.fetchURL(), "application/json", reqBody)
	if err != nil {
		return nil, fmt.Errorf("http error: %v", err)
	}
	// verify server sent no error
	defer response.Body.Close()
	ser, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("server error: %v", err)
	}
	// now read all actions
	var actionList lib.ProtoActionList
	err = proto.Unmarshal(ser, &actionList)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	actions := lib.FromProtoActionList(&actionList)
	return actions, nil
}

// LogAction makes the http request to server to log the given action
func (c *Client) LogAction(action lib.Action) error {
	err := action.Validate()
	if err != nil {
		return fmt.Errorf("can not log invalid action: %v", err)
	}
	pa := lib.ToProtoAction(action)
	ser, err := proto.Marshal(&pa)
	if err != nil {
		return err
	}
	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(c.logURL(), "application/json", reqBody)
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

func (c *Client) GetCount(request lib.GetCountRequest) (uint64, error) {
	err := request.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid request: %v", err)
	}

	// now convert to proto and marshal
	pgcr := lib.ToProtoGetCountRequest(&request)
	ser, err := proto.Marshal(&pgcr)
	if err != nil {
		return 0, fmt.Errorf("could not marshal request: %v", err)
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(c.countURL(), "application/json", reqBody)
	if err != nil {
		return 0, fmt.Errorf("http error: %v", err)
	}
	// verify server sent no error
	defer response.Body.Close()
	ser, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}
	var count uint64
	err = json.Unmarshal(ser, &count)
	if err != nil {
		return 0, fmt.Errorf("server unmarshall error %v", err)
	}
	return count, nil
}

// GetRate returns the normalized ratio of two counters in the same window
// if lower is true, the lower bound is returned and if false upper bound is returned
// TODO: ideally we should just move this logic to server instead of client?
func (c *Client) GetRate(request lib.GetRateRequest) (float64, error) {
	err := request.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid request: %v", err)
	}

	// now convert to proto and marshal
	pgrr := lib.ToProtoGetRateRequest(&request)
	ser, err := proto.Marshal(&pgrr)
	if err != nil {
		return 0, fmt.Errorf("could not marshal request: %v", err)
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(c.rateURL(), "application/json", reqBody)
	if err != nil {
		return 0, fmt.Errorf("http error: %v", err)
	}
	// verify server sent no error
	defer response.Body.Close()
	ser, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}
	var rate float64
	err = json.Unmarshal(ser, &rate)
	if err != nil {
		return 0, fmt.Errorf("server unmarshall error %v", err)
	}
	return rate, nil
}
