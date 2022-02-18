package client

import (
	"bytes"
	"encoding/json"
	"fennel/engine/ast"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	profileLib "fennel/lib/profile"
	"fennel/lib/query"
	"fennel/lib/value"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"

	"google.golang.org/protobuf/proto"
)

type Client struct {
	httpclient *http.Client
	url        *url.URL
}

func NewClient(hostport string, httpclient *http.Client) (*Client, error) {
	url, err := url.Parse(hostport)
	if err != nil {
		return nil, fmt.Errorf("failed to parse hostport [%s]: %v", hostport, err)
	}
	return &Client{
		url:        url,
		httpclient: httpclient,
	}, nil
}

func (c Client) logURL() string {
	c.url.Path = "/log"
	return fmt.Sprintf(c.url.String())
}

func (c Client) fetchURL() string {
	c.url.Path = "/fetch"
	return fmt.Sprintf(c.url.String())
}

func (c Client) queryURL() string {
	c.url.Path = "/query"
	return fmt.Sprintf(c.url.String())
}

func (c Client) getProfileURL() string {
	c.url.Path = "/get"
	return fmt.Sprintf(c.url.String())
}

func (c Client) setProfileURL() string {
	c.url.Path = "/set"
	return fmt.Sprintf(c.url.String())
}

func (c Client) getProfileMultiURL() string {
	c.url.Path = "/get_multi"
	return fmt.Sprintf(c.url.String())
}

func (c Client) storeAggregateURL() string {
	c.url.Path = "/store_aggregate"
	return fmt.Sprintf(c.url.String())
}

func (c Client) retrieveAggregateURL() string {
	c.url.Path = "/retrieve_aggregate"
	return fmt.Sprintf(c.url.String())
}

func (c Client) deactivateAggregateURL() string {
	c.url.Path = "/deactivate_aggregate"
	return fmt.Sprintf(c.url.String())
}

func (c Client) getAggregateValueURL() string {
	c.url.Path = "/aggregate_value"
	return fmt.Sprintf(c.url.String())
}

func (c Client) post(protoMessage proto.Message, url string) ([]byte, error) {
	// serialize the request to be sent on wire
	ser, err := proto.Marshal(protoMessage)
	if err != nil {
		return nil, fmt.Errorf("marshal error on client: %v", err)
	}

	reqBody := bytes.NewBuffer(ser)
	response, err := c.httpclient.Post(url, "application/json", reqBody)
	if err != nil {
		return nil, fmt.Errorf("server error: %v", err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read server response: %v", err)
	}
	// handle http error given by the server
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", http.StatusText(response.StatusCode), string(body))
	}
	return body, nil
}

func (c Client) postJSON(data []byte, url string) ([]byte, error) {
	reqBody := bytes.NewBuffer(data)
	response, err := c.httpclient.Post(url, "application/json", reqBody)
	if err != nil {
		return nil, fmt.Errorf("server error: %v", err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("could not read server response: %v", err)
	}
	// handle http error given by the server
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("%s: %s", http.StatusText(response.StatusCode), string(body))
	}
	return body, nil
}

// GetProfile if no matching value is found, a nil pointer is returned with no error
// If a matching value is found, a valid Value pointer is returned with no error
// If an error occurs, a nil pointer is returned with a non-nil error
func (c *Client) GetProfile(request *profileLib.ProfileItem) (*value.Value, error) {
	// validate and convert to json
	if err := request.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %v", err)
	}
	req, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("could not convert request to json: %v", err)
	}
	response, err := c.postJSON(req, c.getProfileURL())
	if err != nil {
		return nil, err
	}
	// so server sent some response without error, so let's decode that response
	if len(response) == 0 {
		// i.e. no valid value is found, so we return nil pointer
		return nil, nil
	}
	// now try to read response as a JSON object and convert to value
	v, err := value.FromJSON(response)
	if err != nil {
		return nil, err
	} else {
		return &v, nil
	}
}

func (c *Client) Query(reqAst ast.Ast, reqArgs value.Dict) (value.Value, error) {
	// convert the request to proto version
	req, err := query.ToBoundQueryJSON(reqAst, reqArgs)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %v", err)
	}
	response, err := c.postJSON(req, c.queryURL())
	if err != nil {
		return nil, err
	}
	// now try to read response as a JSON object and convert to value
	v, err := value.FromJSON(response)
	if err != nil {
		return nil, fmt.Errorf("error parsing value json: %v", err)
	}
	return v, nil
}

func (c *Client) SetProfile(request *profileLib.ProfileItem) error {
	// validate and convert to json
	if err := request.Validate(); err != nil {
		return fmt.Errorf("invalid request: %v", err)
	}
	req, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("could not convert request to json: %v", err)
	}
	if _, err = c.postJSON(req, c.setProfileURL()); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetProfileMulti(request profileLib.ProfileFetchRequest) ([]profileLib.ProfileItem, error) {
	req, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	response, err := c.postJSON(req, c.getProfileMultiURL())
	if err != nil {
		return nil, err
	}
	// now read all profiles
	var profiles []profileLib.ProfileItem
	if err = json.Unmarshal(response, &profiles); err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	return profiles, nil
}

func (c *Client) FetchActions(request action.ActionFetchRequest) ([]action.Action, error) {
	req, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	response, err := c.postJSON(req, c.fetchURL())
	if err != nil {
		return nil, err
	}
	// now read all actions
	var actions []action.Action
	if err = json.Unmarshal(response, &actions); err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	return actions, nil
}

// LogAction makes the http request to server to log the given action
func (c *Client) LogAction(request action.Action) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("can not log invalid action: %v", err)
	}
	req, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("could not convert request to json: %v", err)
	}
	if _, err = c.postJSON(req, c.logURL()); err != nil {
		return err
	}
	return nil
}

func (c *Client) StoreAggregate(agg aggregate.Aggregate) error {
	if ok := aggregate.IsValid(ftypes.AggType(agg.Options.AggType)); !ok {
		return fmt.Errorf("invalid aggregate type: %v", agg.Options.AggType)
	}

	protoAgg, err := aggregate.ToProtoAggregate(agg)
	if err != nil {
		return err
	}
	_, err = c.post(&protoAgg, c.storeAggregateURL())
	return err
}

func (c *Client) RetrieveAggregate(aggname ftypes.AggName) (aggregate.Aggregate, error) {
	empty := aggregate.Aggregate{}
	if len(aggname) == 0 {
		return empty, fmt.Errorf("aggregate name can not be of length zero")
	}

	// convert to proto request and send to server
	aggreq := aggregate.AggRequest{AggName: string(aggname)}
	response, err := c.post(&aggreq, c.retrieveAggregateURL())
	if err != nil {
		return empty, err
	}
	if len(response) == 0 {
		// i.e no aggregate was found
		return empty, aggregate.ErrNotFound
	}
	// convert server response back to an aggregate tier
	var pret aggregate.ProtoAggregate
	if err = proto.Unmarshal(response, &pret); err != nil {
		return empty, err
	}
	ret, err := aggregate.FromProtoAggregate(pret)
	if err != nil {
		return empty, err
	} else {
		return ret, nil
	}
}

func (c *Client) DeactivateAggregate(aggname ftypes.AggName) error {
	if len(aggname) == 0 {
		return fmt.Errorf("aggregate name can not be of length zero")
	}

	// convert to proto request and send to server
	aggreq := aggregate.AggRequest{AggName: string(aggname)}
	_, err := c.post(&aggreq, c.deactivateAggregateURL())
	return err
}

func (c *Client) GetAggregateValue(aggname ftypes.AggName, key value.Value) (value.Value, error) {
	// convert to proto request and send to server
	aggreq := aggregate.GetAggValueRequest{AggName: aggname, Key: key}
	preq, err := aggregate.ToProtoGetAggValueRequest(aggreq)
	if err != nil {
		return value.Nil, err
	}

	response, err := c.post(&preq, c.getAggregateValueURL())
	if err != nil {
		return value.Nil, err
	}
	// convert server response back to a value object and return
	var ret value.Value
	err = value.Unmarshal(response, &ret)
	return ret, err
}
