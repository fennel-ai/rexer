package client

import (
	"bytes"
	"encoding/json"
	"fennel/lib/action"
	"fennel/lib/counter"
	httplib "fennel/lib/http"
	profileLib "fennel/lib/profile"
	"fennel/lib/value"
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
	return fmt.Sprintf("%s:%d/log", c.url, httplib.PORT)
}

func (c Client) fetchURL() string {
	return fmt.Sprintf("%s:%d/fetch", c.url, httplib.PORT)
}

func (c Client) countURL() string {
	return fmt.Sprintf("%s:%d/count", c.url, httplib.PORT)
}
func (c Client) rateURL() string {
	return fmt.Sprintf("%s:%d/rate", c.url, httplib.PORT)
}
func (c Client) getProfileURL() string {
	return fmt.Sprintf("%s:%d/get", c.url, httplib.PORT)
}
func (c Client) setProfileURL() string {
	return fmt.Sprintf("%s:%d/set", c.url, httplib.PORT)
}

func post(protoMessage proto.Message, url string) ([]byte, error) {
	// serialize the request to be sent on wire
	ser, err := proto.Marshal(protoMessage)
	if err != nil {
		return nil, fmt.Errorf("marshal error on client: %v", err)
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := http.Post(url, "application/json", reqBody)
	if err != nil {
		return nil, fmt.Errorf("server error: %v", err)
	}
	// verify server sent no error
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read server response: %v", err)
	}
	return body, nil
}

// GetProfile if no matching value is found, a nil pointer is returned with no error
// If a matching value is found, a valid Value pointer is returned with no error
// If an error occurs, a nil pointer is returned with a non-nil error
func (c *Client) GetProfile(request *profileLib.ProfileItem) (*value.Value, error) {
	// convert the profile item to proto version
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %v", err)
	}
	protoReq, err := profileLib.ToProtoProfileItem(request)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %v", err)
	}

	response, err := post(&protoReq, c.getProfileURL())
	if err != nil {
		return nil, err
	}
	// so server sent some response without error, so let's decode that response
	if len(response) == 0 {
		// i.e. no valid value is found, so we return nil pointer
		return nil, nil
	}
	// now try to read response as a serialized ProtoValue
	var pv value.PValue
	if err = proto.Unmarshal(response, &pv); err != nil {
		return nil, fmt.Errorf("could not unmarshal server response: %v", err)
	}
	// now convert proto value to real value
	v, err := value.FromProtoValue(&pv)
	if err != nil {
		return nil, err
	} else {
		return &v, nil
	}
}

func (c *Client) SetProfile(req *profileLib.ProfileItem) error {
	// first convert to proto
	if err := req.Validate(); err != nil {
		return fmt.Errorf("invalid request: %v", err)
	}
	protoReq, err := profileLib.ToProtoProfileItem(req)
	if err != nil {
		return fmt.Errorf("could not convert request to proto: %v", err)
	}
	// serialize the request to be sent on wire
	if _, err = post(&protoReq, c.setProfileURL()); err != nil {
		return err
	}
	return nil
}

func (c *Client) FetchActions(request action.ActionFetchRequest) ([]action.Action, error) {
	protoRequest := action.ToProtoActionFetchRequest(request)
	response, err := post(&protoRequest, c.fetchURL())
	if err != nil {
		return nil, err
	}
	// now read all actions
	var actionList action.ProtoActionList
	err = proto.Unmarshal(response, &actionList)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	actions := action.FromProtoActionList(&actionList)
	return actions, nil
}

// LogAction makes the http request to server to log the given action
func (c *Client) LogAction(a action.Action) error {
	err := a.Validate()
	if err != nil {
		return fmt.Errorf("can not log invalid action: %v", err)
	}
	pa := action.ToProtoAction(a)
	if _, err = post(&pa, c.logURL()); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetCount(request counter.GetCountRequest) (uint64, error) {
	err := request.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid request: %v", err)
	}

	protoRequest := counter.ToProtoGetCountRequest(&request)
	response, err := post(&protoRequest, c.countURL())
	if err != nil {
		return 0, err
	}
	var count uint64
	err = json.Unmarshal(response, &count)
	if err != nil {
		return 0, fmt.Errorf("server unmarshall error %v", err)
	}
	return count, nil
}

// GetRate returns the normalized ratio of two counters in the same window
// if lower is true, the lower bound is returned and if false upper bound is returned
func (c *Client) GetRate(request counter.GetRateRequest) (float64, error) {
	err := request.Validate()
	if err != nil {
		return 0, fmt.Errorf("invalid request: %v", err)
	}
	// convert to proto and send to server
	protoRequest := counter.ToProtoGetRateRequest(&request)
	response, err := post(&protoRequest, c.rateURL())
	if err != nil {

		return 0, err
	}
	var rate float64
	err = json.Unmarshal(response, &rate)
	if err != nil {
		return 0, fmt.Errorf("server unmarshall error %v", err)
	}
	return rate, nil
}
