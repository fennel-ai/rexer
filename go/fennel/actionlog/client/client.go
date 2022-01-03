package client

import (
	"bytes"
	"encoding/json"
	"fennel/actionlog/lib"
	"fmt"
	"io/ioutil"
	"net/http"
)

// TODO: this client needs to take two urls - one for dbserver and other for aggregator?
// Or somehow rearchitect such that server can also talk to aggrgator db
type Client struct {
	url string
}

func NewClient(url string) Client {
	return Client{url}
}

func (c Client) LogURL() string {
	return fmt.Sprintf("%s:%d/log", c.url, lib.PORT)
}

func (c Client) FetchURL() string {
	return fmt.Sprintf("%s:%d/fetch", c.url, lib.PORT)
}

func (c Client) CountURL() string {
	return fmt.Sprintf("%s:%d/count", c.url, lib.PORT)
}

func (c *Client) Fetch(request lib.ActionFetchRequest) ([]lib.Action, error) {
	ser, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("could not marshal request: %v", err)
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(c.FetchURL(), "application/json", reqBody)
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
	var actions []lib.Action
	err = json.Unmarshal([]byte(ser), &actions)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	return actions, nil
}

// Log makes the http request to server to log the given action
func (c *Client) Log(action lib.Action) (lib.OidType, error) {
	err := action.Validate()
	if err != nil {
		return 0, fmt.Errorf("can not log invalid action: %v", err)
	}
	ser, err := json.Marshal(action)
	if err != nil {
		return 0, fmt.Errorf("could not marshal action: %v", err)
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(c.LogURL(), "application/json", reqBody)
	if err != nil {
		return 0, fmt.Errorf("http error: %v", err)
	}
	// verify server sent no error
	defer response.Body.Close()
	ser, err = ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}
	var actionId lib.OidType
	err = json.Unmarshal(ser, &actionId)
	if err != nil {
		return 0, fmt.Errorf("server unmarshall error %v", err)
	}
	return actionId, nil
}

func (c *Client) Count(request lib.GetCountRequest) (uint64, error) {
	err := request.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid request: %v", err)
	}
	ser, err := json.Marshal(request)
	if err != nil {
		return 0, fmt.Errorf("could not marshal request: %v", err)
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(c.CountURL(), "application/json", reqBody)
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

// Rate returns the normalized ratio of two counters in the same window
// if lower is true, the lower bound is returned and if false upper bound is returned
// TODO: ideally we should just move this logic to server instead of client?
func (c *Client) Rate(ct1 lib.CounterType, key1 lib.Key, ct2 lib.CounterType, key2 lib.Key,
	window lib.Window, lower bool) (float64, error) {
	num, err := c.Count(lib.GetCountRequest{CounterType: ct1, Window: window, Key: key1})
	if err != nil {
		return 0, err
	}
	den, err := c.Count(lib.GetCountRequest{CounterType: ct2, Window: window, Key: key2})
	if err != nil {
		return 0, err
	}
	ratio := lib.Wilson(num, den, lower)
	return ratio, nil
}
