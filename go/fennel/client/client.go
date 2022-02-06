package client

import (
	"bytes"
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

	response, err := c.post(&protoReq, c.getProfileURL())
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

func (c *Client) Query(req ast.Ast, reqdict value.Dict) (value.Value, error) {
	// convert the request to proto version
	request := query.AstWithDict{Ast: req, Dict: reqdict}
	protoReq, err := query.ToProtoAstWithDict(&request)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %v", err)
	}

	response, err := c.post(&protoReq, c.queryURL())
	if err != nil {
		return nil, err
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
		return v, nil
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
	if _, err = c.post(&protoReq, c.setProfileURL()); err != nil {
		return err
	}
	return nil
}

func (c *Client) GetProfileMulti(request profileLib.ProfileFetchRequest) ([]profileLib.ProfileItem, error) {
	protoRequest := profileLib.ToProtoProfileFetchRequest(&request)
	response, err := c.post(&protoRequest, c.getProfileMultiURL())
	if err != nil {
		return nil, err
	}

	var profileList profileLib.ProtoProfileList
	err = proto.Unmarshal(response, &profileList)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %v", err)
	}
	profiles, err := profileLib.FromProtoProfileList(&profileList)
	if err != nil {
		return nil, err
	}
	return profiles, nil
}

func (c *Client) FetchActions(request action.ActionFetchRequest) ([]action.Action, error) {
	protoRequest := action.ToProtoActionFetchRequest(request)
	response, err := c.post(&protoRequest, c.fetchURL())
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
	if _, err = c.post(&pa, c.logURL()); err != nil {
		return err
	}
	return nil
}

func (c *Client) StoreAggregate(agg aggregate.Aggregate) error {
	if ok := aggregate.IsValid(agg.Type); !ok {
		return fmt.Errorf("invalid aggregate type: %v", agg.Type)
	}

	protoAgg, err := aggregate.ToProtoAggregate(agg)
	if err != nil {
		return err
	}
	_, err = c.post(&protoAgg, c.storeAggregateURL())
	return err
}

func (c *Client) RetrieveAggregate(aggtype ftypes.AggType, aggname ftypes.AggName) (aggregate.Aggregate, error) {
	empty := aggregate.Aggregate{}
	if ok := aggregate.IsValid(aggtype); !ok {
		return empty, fmt.Errorf("invalid aggregate type: %v", aggtype)
	}
	if len(aggname) == 0 {
		return empty, fmt.Errorf("aggregate name can not be of length zero")
	}

	// convert to proto request and send to server
	aggreq := aggregate.AggRequest{AggType: string(aggtype), AggName: string(aggname)}
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

func (c *Client) GetAggregateValue(aggtype ftypes.AggType, aggname ftypes.AggName, key value.Value) (value.Value, error) {
	// convert to proto request and send to server
	aggreq := aggregate.GetAggValueRequest{AggType: aggtype, AggName: aggname, Key: key}
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
