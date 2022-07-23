package airbyte

import (
	"fmt"
	"net/http"
	"net/url"
)

const (
	SOURCE_ID_LIST_PATH        = "v1/source_definitions/list"
	SOURCE_ID_LIST_LATEST_PATH = "v1/source_definitions/list_latest"
	CHECK_CONNECTION_PATH      = "v1/scheduler/sources/check_connection"
	SOURCE_CREATE_PATH         = "v1/sources/create"
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

func (c *Client) CreateSource(source *Source) error {
	return c.getSourceIDList(SOURCE_ID_LIST_PATH)
}

func (c *Client) GetSourceIdList() ([]string, error) {
	return nil, nil
}
