package airbyte

import (
	"encoding/json"
	"fennel/lib/data_integration"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	TEST_SOURCE_ID       = "abc-123-test-source-id"
	KAFKA_DESTINATION_ID = "abc-123-test-destination-id"
	TEST_CONNECTION_ID   = "abc-123-test-connector-id"
)

type testServer struct {
	t *testing.T
}

func (s *testServer) sourceDefinitionHandler(w http.ResponseWriter, r *http.Request) {
	var fields struct {
		Sources []map[string]interface{} `json:"sourceDefinitions"`
	}
	fields.Sources = []map[string]interface{}{
		{
			"name":               "S3",
			"sourceDefinitionId": "test-source-id-s3",
		},
		{
			"name":               "BigQuery",
			"sourceDefinitionId": "test-source-id-bigquery",
		},
	}
	b, _ := json.Marshal(fields)
	_, _ = w.Write(b)
}

func (s *testServer) workspaceListHandler(w http.ResponseWriter, r *http.Request) {
	fields := map[string][]map[string]interface{}{
		"workspaces": {
			{
				"workspaceId": "test-workspace-id",
			},
		},
	}

	b, _ := json.Marshal(fields)
	_, _ = w.Write(b)
}

func (s *testServer) listDestinationsHandler(w http.ResponseWriter, r *http.Request) {
	fields := make(map[string][]Destination, 0)
	fields["destinations"] = []Destination{
		{
			DestinationId: KAFKA_DESTINATION_ID,
			ConnectionConfiguration: KafkaConnectorConfig{
				TopicPattern: "t_123_" + AIRBYTE_KAFKA_TOPIC,
			},
		},
	}
	b, _ := json.Marshal(fields)
	_, _ = w.Write(b)
}

func (s *testServer) createSourceHandler(w http.ResponseWriter, r *http.Request) {
	srcResponse := make(map[string]interface{}, 0)
	srcResponse["sourceId"] = TEST_SOURCE_ID
	b, _ := json.Marshal(srcResponse)
	_, _ = w.Write(b)
}

func (s *testServer) checkConnectionHandler(w http.ResponseWriter, r *http.Request) {
	connResponse := make(map[string]interface{}, 0)
	connResponse["status"] = "succeeded"
	b, _ := json.Marshal(connResponse)
	_, _ = w.Write(b)
}

func (s *testServer) discoverSchemaHandler(w http.ResponseWriter, r *http.Request) {
	var schemaResponse struct {
		Catalog Catalog     `json:"catalog"`
		JobInfo interface{} `json:"jobInfo"`
	}
	schemaResponse.Catalog = Catalog{}
	streamConfig := StreamConfig{
		Stream: Stream{
			Name: "test-stream",
			SupportedSyncModes: []string{
				"incremental",
			},
		},
		Config: MutableSourceConfig{
			SyncMode: "incremental",
		},
	}
	schemaResponse.Catalog.Streams = []StreamConfig{streamConfig}
	b, _ := json.Marshal(schemaResponse)
	_, _ = w.Write(b)
}

func (s *testServer) createConnectorHandler(w http.ResponseWriter, r *http.Request) {
	connResponse := map[string]interface{}{
		"connectionId": TEST_CONNECTION_ID,
	}
	b, _ := json.Marshal(connResponse)
	_, _ = w.Write(b)
}

func newTestServer(t *testing.T, testServer *testServer) *httptest.Server {
	mux := http.NewServeMux()

	// Handlers to set up the airbyte client
	mux.HandleFunc(SOURCE_ID_LIST_PATH, testServer.sourceDefinitionHandler)
	mux.HandleFunc(WORKSPACE_LIST_PATH, testServer.workspaceListHandler)
	mux.HandleFunc(LIST_DESTINATIONS_PATH, testServer.listDestinationsHandler)

	// Handlers to create a source
	mux.HandleFunc(CREATE_SOURCE_PATH, testServer.createSourceHandler)
	mux.HandleFunc(CHECK_CONNECTION_PATH, testServer.checkConnectionHandler)

	// Handlers to create a connector

	mux.HandleFunc(DISCOVER_SOURCE_SCHEMA_PATH, testServer.discoverSchemaHandler)
	mux.HandleFunc(CREATE_CONNECTOR_PATH, testServer.createConnectorHandler)
	return httptest.NewServer(mux)
}

func TestAirbyteSourceClient(t *testing.T) {
	// Setup a mock airbyte server
	Ts := &testServer{
		t: t,
	}
	svr := newTestServer(t, Ts)
	defer svr.Close()
	client, err := NewClient(svr.URL, 123)
	assert.NoError(t, err)
	src := data_integration.S3{
		Name: "test-source",
	}
	srcId, err := client.CreateSource(src)
	assert.Equal(t, TEST_SOURCE_ID, srcId)
	assert.NoError(t, err)
}

func TestAirbyteConnectorClient(t *testing.T) {
	Ts := &testServer{
		t: t,
	}
	svr := newTestServer(t, Ts)
	defer svr.Close()
	client, err := NewClient(svr.URL, 123)
	assert.NoError(t, err)
	src := data_integration.S3{
		Name: "test-source",
	}
	conn := data_integration.Connector{
		Name:        "test-connector",
		SourceName:  "test-source",
		SourceType:  "S3",
		StreamName:  "test-stream",
		Version:     "1.0.0",
		Destination: "profiles",
		CursorField: "timestamp",
		Query:       nil,
		Active:      false,
	}

	connId, err := client.CreateConnector(src, conn)
	assert.NoError(t, err)
	assert.Equal(t, TEST_CONNECTION_ID, connId)
}
