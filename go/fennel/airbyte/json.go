package airbyte

import (
	"fennel/lib/data_integration"
)

// ---------------------------------------------------------------------------------------------------------------------
// Json structs for Connectors
// ---------------------------------------------------------------------------------------------------------------------

type ConnectorConfig struct {
	Name                string   `json:"name"`
	NamespaceDefinition string   `json:"namespaceDefinition"`
	NamespaceFormat     string   `json:"namespaceFormat"`
	Prefix              string   `json:"prefix"`
	SourceId            string   `json:"sourceId"`
	DestinationId       string   `json:"destinationId"`
	SyncCatalog         Catalog  `json:"syncCatalog"`
	Schedule            Schedule `json:"schedule"`
	Status              string   `json:"status"`
}

type UpdateConnectorConfig struct {
	ConnectionId        string   `json:"connectionId"`
	NamespaceDefinition string   `json:"namespaceDefinition"`
	NamespaceFormat     string   `json:"namespaceFormat"`
	Prefix              string   `json:"prefix"`
	SyncCatalog         Catalog  `json:"syncCatalog"`
	Schedule            Schedule `json:"schedule"`
	Status              string   `json:"status"`
}

type Schedule struct {
	Units    int    `json:"units"`
	TimeUnit string `json:"timeUnit"`
}

type Catalog struct {
	Streams []StreamConfig `json:"streams"`
}

type StreamConfig struct {
	Stream Stream              `json:"stream"`
	Config MutableSourceConfig `json:"config"`
}

type Stream struct {
	Name                    string           `json:"name"`
	JsonSchema              StreamJsonSchema `json:"jsonSchema"`
	SupportedSyncModes      []string         `json:"supportedSyncModes"`
	SourceDefinedCursor     bool             `json:"sourceDefinedCursor"`
	DefaultCursorField      []string         `json:"defaultCursorField"`
	SourceDefinedPrimaryKey []string         `json:"sourceDefinedPrimaryKey"`
	// It is a ptr since Namespace can be null and Go defaults to "" for empty string rather than null.
	Namespace *string `json:"namespace"`
}

type StreamJsonSchema struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
}

type MutableSourceConfig struct {
	SyncMode            string   `json:"syncMode"`
	CursorField         []string `json:"cursorField"`
	DestinationSyncMode string   `json:"destinationSyncMode"`
	Selected            bool     `json:"selected"`
}

func (s StreamConfig) SupportIncrementalMode() bool {
	modes := s.Stream.SupportedSyncModes
	if len(modes) == 0 {
		return false
	}
	for _, mode := range modes {
		if mode == "incremental" {
			return true
		}
	}
	return false
}

func (s StreamConfig) HasCursorField(cursorField string) bool {
	for key := range s.Stream.JsonSchema.Properties {
		if key == cursorField {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------------------------------------------------
// Json structs for Sources
// ---------------------------------------------------------------------------------------------------------------------

type SourceConfig struct {
	Name                    string           `json:"name"`
	SourceDefinitionId      string           `json:"sourceDefinitionId"`
	WorkspaceId             string           `json:"workspaceId"`
	ConnectionConfiguration ConnectionConfig `json:"connectionConfiguration"`
}

type CheckConnectionRequest struct {
	SourceDefinitionId      string           `json:"sourceDefinitionId"`
	ConnectionConfiguration ConnectionConfig `json:"connectionConfiguration"`
}

type ConnectionConfig interface {
	GetSourceType() string
}

// S3 Info

type S3ConnectorConfig struct {
	Dataset     string     `json:"dataset"`
	Format      S3Format   `json:"format"`
	PathPattern string     `json:"path_pattern"`
	Provider    S3Provider `json:"provider"`
	Schema      string     `json:"schema"`
}

func NewS3ConnectorConfig(src data_integration.S3) (S3ConnectorConfig, error) {
	config := S3ConnectorConfig{
		PathPattern: "**",
		Schema:      "{}",
	}
	switch src.Format {
	case "parquet":
		config.Format = NewS3ParquetConfig(src)
	case "avro":
		config.Format = NewS3AvroConfig()
	default:
		config.Format = NewS3CSVConfig(src)
	}
	return config, nil
}

func (s S3ConnectorConfig) GetSourceType() string {
	return "S3"
}

type S3Format interface {
	GetFileType() string
}

type S3CSVFormat struct {
	AdditionalReaderOptions string `json:"additional_reader_options"`
	AdvancedOptions         string `json:"advanced_options"`
	BlockSize               int    `json:"block_size"`
	Delimiter               string `json:"delimiter"`
	DoubleQuote             bool   `json:"double_quote"`
	Encoding                string `json:"encoding"`
	FileType                string `json:"filetype"`
}

func (c S3CSVFormat) GetFileType() string {
	return c.FileType
}

type S3ParquetFormat struct {
	BatchSize  int    `json:"batch_size"`
	BufferSize int    `json:"buffer_size"`
	FileType   string `json:"filetype"`
}

func (c S3ParquetFormat) GetFileType() string {
	return c.FileType
}

func NewS3ParquetConfig(src data_integration.S3) S3ParquetFormat {
	return S3ParquetFormat{
		BatchSize:  65536,
		BufferSize: 2,
		FileType:   "parquet",
	}
}

type S3AvroFormat struct {
	FileType string `json:"filetype"`
}

func (c S3AvroFormat) GetFileType() string {
	return c.FileType
}

func NewS3AvroConfig() S3AvroFormat {
	return S3AvroFormat{
		FileType: "avro",
	}
}

func NewS3CSVConfig(src data_integration.S3) S3CSVFormat {
	if src.Delimiter == "" {
		src.Delimiter = ","
	}
	return S3CSVFormat{
		AdditionalReaderOptions: "{}",
		AdvancedOptions:         "{}",
		BlockSize:               10000,
		Delimiter:               src.Delimiter,
		DoubleQuote:             true,
		Encoding:                "utf-8",
		FileType:                "csv",
	}
}

type S3Provider struct {
	AWSAccessKeyId     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
	Bucket             string `json:"bucket"`
	PathPrefix         string `json:"path_prefix"`
	Endpoint           string `json:"endpoint"`
}

// Big Query Info

type BigQueryConnectorConfig struct {
	DatasetId       string `json:"dataset_id"`
	ProjectId       string `json:"project_id"`
	CredentialsJson string `json:"credentials_json"`
}

func (b BigQueryConnectorConfig) GetSourceType() string {
	return "BigQuery"
}

// ---------------------------------------------------------------------------------------------------------------------
// Helper Json structs for destination
// ---------------------------------------------------------------------------------------------------------------------

type Destination struct {
	DestinationDefinitionId string               `json:"destinationDefinitionId"`
	DestinationId           string               `json:"destinationId"`
	WorkspaceId             string               `json:"workspaceId"`
	ConnectionConfiguration KafkaConnectorConfig `json:"connectionConfiguration"`
	Name                    string               `json:"name"`
	DestinationName         string               `json:"destinationName"`
}

type KafkaConnectorConfig struct {
	Acks                           string   `json:"acks"`
	Retries                        int      `json:"retries"`
	Protocol                       Protocol `json:"protocol"`
	LingerMs                       string   `json:"linger_ms"`
	BathSize                       int      `json:"batch_size"`
	TestTopic                      string   `json:"test_topic"`
	MaxBlockMs                     string   `json:"max_block_ms"`
	BufferMemory                   string   `json:"buffer_memory"`
	SyncProducer                   bool     `json:"sync_producer"`
	TopicPattern                   string   `json:"topic_pattern"`
	CompressionType                string   `json:"compression_type"`
	MaxRequestSize                 int      `json:"max_request_size"`
	BootstrapServers               string   `json:"bootstrap_servers"`
	ClientDnsLookup                string   `json:"client_dns_lookup"`
	SendBufferBytes                int      `json:"send_buffer_bytes"`
	EnableIdempotence              bool     `json:"enable_idempotence"`
	RequestTimeoutMs               int      `json:"request_timeout_ms"`
	DeliveryTimeoutMs              int      `json:"delivery_timeout_ms"`
	ReceiveBufferBytes             int      `json:"receive_buffer_bytes"`
	SocketConnectTimeoutMs         string   `json:"socket_connect_timeout_ms"`
	MaxInFlightRequests            int      `json:"max_in_flight_requests"`
	SocketConnectionSetupTimeoutMs string   `json:"socket_connection_setup_timeout_ms"`
}

type Protocol struct {
	SaslMechanism    string `json:"sasl_mechanism"`
	SaslJaasConfig   string `json:"sasl_jaas_config"`
	SecurityProtocol string `json:"security_protocol"`
}
