package airbyte

import (
	"bytes"
	"encoding/json"
	"fennel/lib/data_integration"
	"fennel/lib/ftypes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"time"
)

const (
	SOURCE_ID_LIST_PATH          = "/v1/source_definitions/list"
	SOURCE_ID_LIST_LATEST_PATH   = "/v1/source_definitions/list_latest"
	CHECK_CONNECTION_PATH        = "/v1/scheduler/sources/check_connection"
	CREATE_SOURCE_PATH           = "/v1/sources/create"
	WORKSPACE_LIST_PATH          = "/v1/workspaces/list"
	DISCOVER_SOURCE_SCHEMA_PATH  = "/v1/sources/discover_schema"
	CREATE_CONNECTOR_PATH        = "/v1/connections/create"
	UPDATE_CONNECTOR_PATH        = "/v1/connections/update"
	LIST_DESTINATIONS_PATH       = "/v1/destinations/list"
	DELETE_CONNECTOR_PATH        = "/v1/connections/delete"
	DELETE_SOURCE_PATH           = "/v1/sources/delete"
	LIST_DESTINATION_ID_PATH     = "/v1/destination_definitions/list"
	CHECK_DESTINATION_CONNECTION = "/v1/scheduler/destinations/check_connection"
	CREATE_DESTINATION_PATH      = "/v1/destinations/create"
)

const (
	REFRESH_FREQUENCY_MINUTES = 5
	AIRBYTE_KAFKA_TOPIC       = "streamlog"
	PROFILE_DESTINATION       = "profile"
	ACTION_DESTINATION        = "action"
	AIRBYTE_DEDUP_TTL         = 30 * time.Minute
)

type Client struct {
	httpclient *http.Client
	url        *url.URL
}

var sourceDefinitionIdCache map[string]string
var workspaceId string
var kafkaDestinationId string

func init() {
	sourceDefinitionIdCache = make(map[string]string)
}

type KafkaCredentials struct {
	Username string
	Password string
	Server   string
}

func NewClient(hostport string, tierId ftypes.RealmID, cred KafkaCredentials) (Client, error) {
	url, err := url.Parse(hostport)
	if err != nil {
		return Client{}, fmt.Errorf("failed to parse hostport [%s]: %v", hostport, err)
	}

	c := Client{
		url:        url,
		httpclient: &http.Client{},
	}

	err = c.setWorkspace()
	if err != nil || workspaceId == "" {
		return Client{}, fmt.Errorf("failed to set workspace: %w", err)
	}
	err = c.setKafkaDestinationId(tierId, cred, 0)
	if err != nil || kafkaDestinationId == "" {
		return Client{}, fmt.Errorf("failed to set kafka destination id: %w", err)
	}
	return c, err
}

// CreateSource creates a source in the Airbyte server and returns the source ID
func (c Client) CreateSource(source data_integration.Source) (string, error) {
	// Get Source ID for source
	sourceId, err := c.getSourceDefinitionId(reflect.TypeOf(source).Name())
	if err != nil {
		return "", err
	}

	// Check if connection can be established
	srcConfig, err := getConnectionConfiguration(source, sourceId)
	if err != nil {
		return "", err
	}
	if err = c.checkConnection(srcConfig); err != nil {
		return "", err
	}

	// Create source
	return c.createSource(srcConfig)
}

func (c Client) DeleteSource(source data_integration.Source) error {
	var fields struct {
		SrcId string `json:"sourceId"`
	}
	fields.SrcId = source.GetSourceId()
	req, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("failed to marshal source id: %w", err)
	}
	if _, err := c.postJSON(req, c.getURL(DELETE_SOURCE_PATH)); err != nil {
		return fmt.Errorf("failed to delete source: %w", err)
	}
	return nil
}

func (c Client) CreateConnector(source data_integration.Source, conn data_integration.Connector) (string, error) {
	// Set Cursor Field for source
	if err := setCursorField(source, &conn); err != nil {
		return "", err
	}

	// Discover schema of the source
	streamConfig, err := c.getSourceSchema(source, conn)
	if err != nil {
		return "", fmt.Errorf("failed to discover schema of source: %w", err)
	}
	// Create connector
	return c.createConnector(conn, source, streamConfig)
}

func (c Client) EnableConnector(source data_integration.Source, conn data_integration.Connector) error {
	// Set Cursor Field for source
	if err := setCursorField(source, &conn); err != nil {
		return err
	}

	// Discover schema of the source
	streamConfig, err := c.getSourceSchema(source, conn)
	if err != nil {
		return fmt.Errorf("failed to discover schema of source: %w", err)
	}
	// Enable connector
	if err = c.updateConnector(conn, streamConfig, "active"); err != nil {
		return fmt.Errorf("failed to enable connector: %w", err)
	}
	return nil
}

func (c Client) DisableConnector(source data_integration.Source, conn data_integration.Connector) error {
	// Set Cursor Field for source
	if err := setCursorField(source, &conn); err != nil {
		return err
	}

	// Discover schema of the source
	streamConfig, err := c.getSourceSchema(source, conn)
	if err != nil {
		return fmt.Errorf("failed to discover schema of source: %w", err)
	}
	if err = c.updateConnector(conn, streamConfig, "inactive"); err != nil {
		return fmt.Errorf("failed to disable connector: %w", err)
	}
	return nil
}

func (c Client) UpdateConnector(source data_integration.Source, conn data_integration.Connector) error {
	// Set Cursor Field for source
	if err := setCursorField(source, &conn); err != nil {
		return err
	}

	// Discover schema of the source
	streamConfig, err := c.getSourceSchema(source, conn)
	if err != nil {
		return fmt.Errorf("failed to discover schema of source: %w", err)
	}
	if err = c.updateConnector(conn, streamConfig, "active"); err != nil {
		return fmt.Errorf("failed to disable connector: %w", err)
	}
	return nil
}

func (c Client) DeleteConnector(conn data_integration.Connector) error {
	// Delete connector
	var fields struct {
		ConnId string `json:"connectionId"`
	}
	fields.ConnId = conn.ConnId
	req, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("failed to marshal connection id: %w", err)
	}
	if _, err := c.postJSON(req, c.getURL(DELETE_CONNECTOR_PATH)); err != nil {
		return fmt.Errorf("failed to delete connector: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Helper functions for Airbyte Connectors
// ---------------------------------------------------------------------------------------------------------------------

func (c Client) createConnector(conn data_integration.Connector, source data_integration.Source, streamConfig StreamConfig) (string, error) {
	if kafkaDestinationId == "" {
		return "", fmt.Errorf("kafka destination id is not set, system is not initialized")
	}

	connConfig := ConnectorConfig{Name: conn.Name, NamespaceDefinition: "source", NamespaceFormat: "${SOURCE_NAMESPACE}", Prefix: ""}
	connConfig.SourceId = source.GetSourceId()
	// TODO: Check if cursor field is among the stream config fields
	streamConfig.Config.CursorField = []string{conn.CursorField}
	streamConfig.Config.SyncMode = "incremental"
	streamConfig.Config.Selected = true
	connConfig.SyncCatalog.Streams = []StreamConfig{streamConfig}
	connConfig.DestinationId = kafkaDestinationId
	connConfig.ScheduleType = "basic"
	connConfig.ScheduleData = ScheduleData{
		BasicSchedule: BasicSchedule{
			Units:    REFRESH_FREQUENCY_MINUTES,
			TimeUnit: "minutes",
		},
	}
	connConfig.Status = "active"
	request, err := json.Marshal(connConfig)
	if err != nil {
		return "", err
	}
	resp, err := c.postJSON(request, c.getURL(CREATE_CONNECTOR_PATH))
	if err != nil {
		return "", err
	}
	var connResponse map[string]interface{}
	err = json.Unmarshal(resp, &connResponse)
	if err != nil {
		return "", err
	}
	if connectionId, ok := connResponse["connectionId"]; ok {
		return connectionId.(string), nil
	}
	// This should not happen
	return "", fmt.Errorf("something went wrong during connection creation")
}

func (c Client) updateConnector(conn data_integration.Connector, streamConfig StreamConfig, state string) error {
	if kafkaDestinationId == "" {
		return fmt.Errorf("kafka destination id is not set, system is not initialized")
	}

	connConfig := UpdateConnectorConfig{NamespaceDefinition: "source", NamespaceFormat: "${SOURCE_NAMESPACE}", Prefix: ""}
	// TODO: Check if cursor field is among the stream config fields
	streamConfig.Config.CursorField = []string{conn.CursorField}
	streamConfig.Config.SyncMode = "incremental"
	streamConfig.Config.Selected = true
	connConfig.SyncCatalog.Streams = []StreamConfig{streamConfig}
	connConfig.ConnectionId = conn.ConnId
	connConfig.ScheduleType = "basic"
	connConfig.ScheduleData = ScheduleData{
		BasicSchedule: BasicSchedule{
			Units:    REFRESH_FREQUENCY_MINUTES,
			TimeUnit: "minutes",
		},
	}
	connConfig.Status = state
	request, err := json.Marshal(connConfig)
	if err != nil {
		return err
	}
	if _, err := c.postJSON(request, c.getURL(UPDATE_CONNECTOR_PATH)); err != nil {
		return fmt.Errorf("failed to update connector: %w", err)
	}
	return nil
}

// getSourceSchema returns the JSON schema of the source. If there are multiple streams we use conn.StreamName to
// determine which stream to use.
func (c Client) getSourceSchema(source data_integration.Source, conn data_integration.Connector) (StreamConfig, error) {
	var fields struct {
		SourceId     string `json:"sourceId"`
		DisableCache bool   `json:"disable_cache"`
	}
	fields.SourceId = source.GetSourceId()
	fields.DisableCache = true
	data, err := json.Marshal(fields)
	if err != nil {
		return StreamConfig{}, err
	}
	resp, err := c.postJSON(data, c.getURL(DISCOVER_SOURCE_SCHEMA_PATH))
	if err != nil {
		return StreamConfig{}, err
	}
	var schemaResponse struct {
		Catalog Catalog     `json:"catalog"`
		JobInfo interface{} `json:"jobInfo"`
	}
	err = json.Unmarshal(resp, &schemaResponse)
	if err != nil {
		return StreamConfig{}, fmt.Errorf("failed to unmarshal discover schema response : %w", err)
	}
	streams := schemaResponse.Catalog.Streams
	if len(streams) == 0 {
		return StreamConfig{}, fmt.Errorf("no schema for the source was found, please ensure the source is properly configured")
	}

	// This should not happen as we check for this in the client.
	if conn.StreamName == "" {
		return StreamConfig{}, fmt.Errorf("stream name is not set in the connector")
	}

	for _, stream := range streams {
		if stream.SupportIncrementalMode() && stream.Stream.Name == conn.StreamName {
			// Check if stream has the specified cursor field
			if !stream.HasCursorField(conn.CursorField) {
				return StreamConfig{}, fmt.Errorf("stream %s does not have the cursor field %s", stream.Stream.Name, conn.CursorField)
			}
			return stream, nil
		}
	}
	return StreamConfig{}, fmt.Errorf("no valid schema for the source was found, please ensure the source is properly configured or the stream name is correctly set")
}

func setCursorField(source data_integration.Source, conn *data_integration.Connector) error {
	if conn.CursorField != "" {
		return nil
	}

	if cursorField := source.GetDefaultCursorField(); cursorField != "" {
		conn.CursorField = cursorField
		return nil
	}

	return fmt.Errorf(reflect.TypeOf(source).Name() + " does not support a default cursor field, please specify a cursor field")
}

// ---------------------------------------------------------------------------------------------------------------------
// Helper functions for Airbyte Sources
// ---------------------------------------------------------------------------------------------------------------------

func (c *Client) createSource(srcConfig SourceConfig) (string, error) {
	data, err := json.Marshal(srcConfig)
	if err != nil {
		return "", err
	}
	resp, err := c.postJSON(data, c.getURL(CREATE_SOURCE_PATH))
	if err != nil {
		return "", err
	}
	var srcResponse map[string]interface{}
	err = json.Unmarshal(resp, &srcResponse)
	if err != nil {
		return "", err
	}

	if srcId, ok := srcResponse["sourceId"]; ok {
		return srcId.(string), nil
	}
	// This should not happen
	return "", fmt.Errorf("something went wrong during source creation")
}

func getConnectionConfiguration(source data_integration.Source, sourceDefId string) (SourceConfig, error) {
	var srcConfig SourceConfig
	srcConfig.Name = source.GetSourceName()
	srcConfig.SourceDefinitionId = sourceDefId
	srcConfig.WorkspaceId = workspaceId
	switch src := source.(type) {
	case data_integration.S3:
		s3ConnectorConfig, err := NewS3ConnectorConfig(src)
		if err != nil {
			return srcConfig, err
		}
		s3ConnectorConfig.Dataset = src.GetSourceName()
		s3ConnectorConfig.Provider.AWSAccessKeyId = src.AWSAccessKeyId
		s3ConnectorConfig.Provider.AWSSecretAccessKey = src.AWSSecretAccessKey
		s3ConnectorConfig.Provider.Bucket = src.Bucket
		s3ConnectorConfig.Provider.PathPrefix = src.PathPrefix
		srcConfig.ConnectionConfiguration = s3ConnectorConfig
	case data_integration.BigQuery:
		bigQueryConnectorConfig := BigQueryConnectorConfig{}
		bigQueryConnectorConfig.ProjectId = src.ProjectId
		bigQueryConnectorConfig.DatasetId = src.DatasetId
		bigQueryConnectorConfig.CredentialsJson = src.CredentialsJson
		srcConfig.ConnectionConfiguration = bigQueryConnectorConfig
	case data_integration.Postgres:
		postgresConnectorConfig := NewPostgresConnectorConfig(src)
		srcConfig.ConnectionConfiguration = postgresConnectorConfig
	case data_integration.MySQL:
		mysqlConnectorConfig := NewMySQLConnectorConfig(src)
		srcConfig.ConnectionConfiguration = mysqlConnectorConfig
	case data_integration.Snowflake:
		snowflakeConnectorConfig := NewSnowflakeConnectorConfig(src)
		srcConfig.ConnectionConfiguration = snowflakeConnectorConfig
	default:
		return srcConfig, fmt.Errorf("source type %s not supported", reflect.TypeOf(source).Name())
	}
	return srcConfig, nil
}

func (c *Client) checkConnection(srcConfig SourceConfig) error {
	connRequest := CheckConnectionRequest{}
	connRequest.ConnectionConfiguration = srcConfig.ConnectionConfiguration
	connRequest.SourceDefinitionId = srcConfig.SourceDefinitionId
	data, err := json.Marshal(connRequest)
	if err != nil {
		return err
	}
	resp, err := c.postJSON(data, c.getURL(CHECK_CONNECTION_PATH))
	if err != nil {
		return err
	}
	var connResponse map[string]interface{}
	err = json.Unmarshal(resp, &connResponse)
	if err != nil {
		return err
	}
	if connResponse["status"] == "succeeded" {
		return nil
	}
	return fmt.Errorf("connection check failed: %s", connResponse["message"])
}

func (c *Client) getSourceDefinitionId(sourceType string) (string, error) {
	if sourceId, ok := sourceDefinitionIdCache[sourceType]; ok {
		return sourceId, nil
	}

	resp, err := c.postJSON([]byte{}, c.getURL(SOURCE_ID_LIST_PATH))
	if err != nil {
		return "", err
	}
	if err = fillSourceDefinitionIdCache(resp); err != nil {
		return "", err
	}
	if sourceId, ok := sourceDefinitionIdCache[sourceType]; ok {
		return sourceId, nil
	}

	// Try the list path to see if source type is in the list
	resp, err = c.postJSON([]byte{}, c.getURL(SOURCE_ID_LIST_LATEST_PATH))
	if err != nil {
		return "", err
	}
	if err = fillSourceDefinitionIdCache(resp); err != nil {
		return "", err
	}
	if sourceId, ok := sourceDefinitionIdCache[sourceType]; ok {
		return sourceId, nil
	}
	return "", fmt.Errorf("source type %s not found among list of supported source", sourceType)
}

func fillSourceDefinitionIdCache(data []byte) error {
	var fields struct {
		Sources []map[string]interface{} `json:"sourceDefinitions"`
	}
	err := json.Unmarshal(data, &fields)
	if err != nil {
		return err
	}
	for _, source := range fields.Sources {
		sourceDefinitionIdCache[source["name"].(string)] = source["sourceDefinitionId"].(string)
	}
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------
// Other helper functions
// ---------------------------------------------------------------------------------------------------------------------

// TODO: create workspace if no workspace is found
func (c Client) setWorkspace() error {
	resp, err := c.postJSON([]byte{}, c.getURL(WORKSPACE_LIST_PATH))
	if err != nil {
		return err
	}
	var fields map[string][]map[string]interface{}
	if err = json.Unmarshal(resp, &fields); err != nil {
		return err
	}

	if len(fields["workspaces"]) == 0 {
		return fmt.Errorf("no workspaces found")
	}
	if len(fields["workspaces"]) > 1 {
		return fmt.Errorf("multiple workspaces found")
	}
	workspaceId = fields["workspaces"][0]["workspaceId"].(string)
	return nil
}

func (c Client) findDestinationDefinitionId() (string, error) {
	var workspace struct {
		WorkspaceId string `json:"workspaceId"`
	}
	workspace.WorkspaceId = workspaceId
	data, err := json.Marshal(workspace)
	if err != nil {
		return "", err
	}
	resp, err := c.postJSON(data, c.getURL(LIST_DESTINATION_ID_PATH))
	if err != nil {
		return "", err
	}
	var fields struct {
		Sources []map[string]interface{} `json:"destinationDefinitions"`
	}
	err = json.Unmarshal(resp, &fields)
	if err != nil {
		return "", err
	}
	for _, source := range fields.Sources {
		if source["name"].(string) == "Kafka" {
			return source["destinationDefinitionId"].(string), nil
		}
	}
	return "", fmt.Errorf("destination type Kafka not found among list of supported destinations")
}

func (c Client) createKafkaDestination(tierId ftypes.RealmID, cred KafkaCredentials) error {
	fmt.Println("Creating Kafka destination for tier", tierId)
	destinationDefinitionid, err := c.findDestinationDefinitionId()
	if destinationDefinitionid == "" || err != nil {
		return fmt.Errorf("destination type Kafka not found among list of supported destinations: %w", err)
	}

	//Check Connection
	var fields struct {
		DestinationDefinitionId string               `json:"destinationDefinitionId"`
		ConnectionConfiguration KafkaConnectorConfig `json:"connectionConfiguration"`
	}
	fields.DestinationDefinitionId = destinationDefinitionid
	fields.ConnectionConfiguration = NewKafkaConnectorConfig(getFullAirbyteKafkaTopic(tierId), cred)
	data, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	resp, err := c.postJSON(data, c.getURL(CHECK_DESTINATION_CONNECTION))
	if err != nil {
		return fmt.Errorf("error checking destination connection: %w", err)
	}
	var connResponse map[string]interface{}
	err = json.Unmarshal(resp, &connResponse)
	if err != nil {
		return err
	}
	if connResponse["status"] != "succeeded" {
		return fmt.Errorf("failure while creating destination connection: %s", connResponse["message"])
	}

	//Create Destination
	var fields2 struct {
		Name                    string               `json:"name"`
		DestinationDefinitionId string               `json:"destinationDefinitionId"`
		WorkspaceId             string               `json:"workspaceId"`
		ConnectionConfiguration KafkaConnectorConfig `json:"connectionConfiguration"`
	}
	fields2.Name = "Fennel Kafka Destination"
	fields2.DestinationDefinitionId = destinationDefinitionid
	fields2.WorkspaceId = workspaceId
	fields2.ConnectionConfiguration = NewKafkaConnectorConfig(getFullAirbyteKafkaTopic(tierId), cred)
	data, err = json.Marshal(fields2)
	if err != nil {
		return err
	}
	_, err = c.postJSON(data, c.getURL(CREATE_DESTINATION_PATH))
	return err
}

// TODO: create Kafka destination if no destination is found
func (c Client) setKafkaDestinationId(tierId ftypes.RealmID, cred KafkaCredentials, retry int) error {
	var workspace struct {
		WorkspaceId string `json:"workspaceId"`
	}
	workspace.WorkspaceId = workspaceId
	data, err := json.Marshal(workspace)
	if err != nil {
		return err
	}
	resp, err := c.postJSON(data, c.getURL(LIST_DESTINATIONS_PATH))
	if err != nil {
		return err
	}
	var destinationList map[string][]Destination
	err = json.Unmarshal(resp, &destinationList)
	if err != nil {
		return err
	}

	for _, destination := range destinationList["destinations"] {
		if destination.ConnectionConfiguration.TopicPattern == getFullAirbyteKafkaTopic(tierId) {
			kafkaDestinationId = destination.DestinationId
			return nil
		}
	}

	fmt.Println("No destinations found, creating one, try: #", retry)
	if retry > 1 {
		return fmt.Errorf("failed to create Kafka destination after %d retries", retry)
	}

	if err = c.createKafkaDestination(tierId, cred); err != nil {
		return fmt.Errorf("failed to create Kafka destination: %s", err)
	}
	if err = c.setKafkaDestinationId(tierId, cred, retry+1); err != nil {
		return fmt.Errorf("failed to set Kafka destination id: %s", err)
	}

	return fmt.Errorf("no valid kafka destination found")
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

func (c Client) getURL(path string) string {
	url := *c.url
	url.Path = url.Path + path
	return url.String()
}

func getFullAirbyteKafkaTopic(tierId ftypes.RealmID) string {
	return fmt.Sprintf("t_%d_%s", tierId, AIRBYTE_KAFKA_TOPIC)
}
