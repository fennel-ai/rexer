package data_integration

import (
	"encoding/json"
	"errors"
	"fmt"
)

const (
	S3_CURSOR_FIELD = "_ab_source_file_last_modified"
)

var ErrSrcNotFound = errors.New("source not found")

// Every derived source must include the following fields:
// Name: the name of the source
// SourceId: Id returned by Airbyte
type Source interface {
	Validate() error
	GetSourceName() string
	GetSourceId() string
	GetDefaultCursorField() string
	Equals(Source) error
}

var _ Source = S3{}
var _ Source = BigQuery{}
var _ Source = Postgres{}
var _ Source = MySQL{}
var _ Source = Snowflake{}

type S3 struct {
	Name               string `db:"name" json:"name"`
	SourceId           string `db:"source_id" json:"source_id"`
	Bucket             string `db:"bucket" json:"bucket"`
	PathPrefix         string `db:"path_prefix" json:"path_prefix"`
	Format             string `db:"format" json:"format"`
	Delimiter          string `db:"delimiter" json:"delimiter"`
	Schema             string `db:"json_schema" json:"schema"`
	AWSAccessKeyId     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
	LastUpdated        string `db:"last_updated" json:"last_updated"`
}

func (s S3) GetSourceName() string {
	return s.Name
}

func (s S3) GetSourceId() string {
	return s.SourceId
}

func (s S3) GetDefaultCursorField() string {
	return S3_CURSOR_FIELD
}

func (s S3) Equals(src Source) error {
	if src.GetSourceName() != s.Name {
		return fmt.Errorf("source name mismatch")
	}
	if s2, ok := src.(S3); ok {
		if s.Bucket == s2.Bucket && s.PathPrefix == s2.PathPrefix && s.Format == s2.Format && s.Delimiter == s2.Delimiter {
			return nil
		}
		return fmt.Errorf("s3 fields do not match")
	} else {
		return fmt.Errorf("source type mismatch")
	}
}

func isJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

func (s S3) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("source name is required")
	}
	if s.Format != "csv" && s.Format != "parquet" && s.Format != "avro" {
		return fmt.Errorf("invalid format: %s we only support csv, parquet, or avro", s.Format)
	}
	if s.Delimiter != "," && s.Delimiter != "|" && s.Delimiter != "\t" {
		return fmt.Errorf("invalid delimiter: %s", s.Delimiter)
	}
	if len(s.Bucket) == 0 {
		return fmt.Errorf("s3 bucket is required")
	}
	if len(s.PathPrefix) == 0 {
		return fmt.Errorf("s3 prefix is required")
	}

	if !isJSON(s.Schema) {
		return fmt.Errorf("schema must be valid json")
	}
	return nil
}

type BigQuery struct {
	Name            string `db:"name" json:"name"`
	SourceId        string `db:"source_id" json:"source_id"`
	ProjectId       string `db:"project_id" json:"project_id"`
	DatasetId       string `db:"dataset_id" json:"dataset_id"`
	LastUpdated     string `db:"last_updated" json:"last_updated"`
	CredentialsJson string `json:"credentials_json"`
}

func (s BigQuery) GetSourceName() string {
	return s.Name
}

func (s BigQuery) GetSourceId() string {
	return s.SourceId
}

// BigQuery does not have a default cursor field
func (s BigQuery) GetDefaultCursorField() string {
	return ""
}

func (s BigQuery) Equals(src Source) error {
	if src.GetSourceName() != s.Name {
		return fmt.Errorf("source name mismatch")
	}
	if s2, ok := src.(BigQuery); ok {
		if s.ProjectId == s2.ProjectId && s.DatasetId == s2.DatasetId {
			return nil
		}
		return fmt.Errorf("bigquery fields do not match")
	} else {
		return fmt.Errorf("source type mismatch")
	}
}

func (s BigQuery) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if len(s.ProjectId) == 0 {
		return fmt.Errorf("project_id is required")
	}
	if len(s.DatasetId) == 0 {
		return fmt.Errorf("dataset_id is required")
	}
	return nil
}

type SQLSource struct {
	Name        string `db:"name" json:"name"`
	SourceId    string `db:"source_id" json:"source_id"`
	Host        string `db:"host" json:"host"`
	Dbname      string `db:"db_name" json:"db_name"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	JdbcParams  string `db:"jdbc_params" json:"jdbc_params"`
	Port        int    `db:"port" json:"port"`
	LastUpdated string `db:"last_updated" json:"last_updated"`
}

type Postgres struct {
	SQLSource
}

func (s Postgres) GetSourceName() string {
	return s.Name
}

func (s Postgres) GetSourceId() string {
	return s.SourceId
}

func (s Postgres) GetDefaultCursorField() string {
	return ""
}

func (s Postgres) Equals(src Source) error {
	if src.GetSourceName() != s.Name {
		return fmt.Errorf("source name mismatch")
	}
	if s2, ok := src.(Postgres); ok {
		if s.Host == s2.Host && s.Dbname == s2.Dbname {
			return nil
		}
		return fmt.Errorf("postgres fields do not match")
	} else {
		return fmt.Errorf("source type mismatch")
	}
}

func (s Postgres) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if s.Host == "" {
		return fmt.Errorf("host is required")
	}
	if s.Dbname == "" {
		return fmt.Errorf("dbname is required")
	}
	return nil
}

type MySQL struct {
	SQLSource
}

func (s MySQL) GetSourceName() string {
	return s.Name
}

func (s MySQL) GetSourceId() string {
	return s.SourceId
}

func (s MySQL) Equals(src Source) error {
	if src.GetSourceName() != s.Name {
		return fmt.Errorf("source name mismatch")
	}
	if s2, ok := src.(MySQL); ok {
		if s.Host == s2.Host && s.Dbname == s2.Dbname {
			return nil
		}
		return fmt.Errorf("mysql fields do not match")
	} else {
		return fmt.Errorf("source type mismatch")
	}
}

func (s MySQL) GetDefaultCursorField() string {
	return ""
}

func (s MySQL) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if s.Host == "" {
		return fmt.Errorf("host is required")
	}
	if s.Dbname == "" {
		return fmt.Errorf("dbname is required")
	}
	return nil
}

type Snowflake struct {
	SQLSource
	Warehouse string `db:"warehouse" json:"warehouse"`
	Role      string `db:"role" json:"role"`
	Schema    string `db:"db_schema" json:"schema"`
}

func (s Snowflake) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("name is required")
	}
	if s.Host == "" {
		return fmt.Errorf("host/account is required")
	}
	if s.Dbname == "" {
		return fmt.Errorf("dbname is required")
	}
	if s.Warehouse == "" {
		return fmt.Errorf("warehouse is required")
	}
	if s.Role == "" {
		return fmt.Errorf("role is required")
	}
	if s.Schema == "" {
		return fmt.Errorf("schema is required")
	}
	return nil
}

func (s Snowflake) GetSourceName() string {
	return s.Name
}

func (s Snowflake) GetSourceId() string {
	return s.SourceId
}

func (s Snowflake) GetDefaultCursorField() string {
	return ""
}

func (s Snowflake) Equals(src Source) error {
	if src.GetSourceName() != s.Name {
		return fmt.Errorf("source name mismatch")
	}
	if s2, ok := src.(Snowflake); ok {
		if s.Host == s2.Host && s.Dbname == s2.Dbname && s.Warehouse == s2.Warehouse && s.Role == s2.Role && s.Schema == s2.Schema {
			return nil
		}
		return fmt.Errorf("snowflake fields do not match")
	} else {
		return fmt.Errorf("source type mismatch")
	}
}
