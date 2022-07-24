package data_integration

import (
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

type S3 struct {
	Name               string `db:"name" json:"name"`
	SourceId           string `db:"source_id" json:"source_id"`
	Bucket             string `db:"bucket" json:"bucket"`
	PathPrefix         string `db:"path_prefix" json:"path_prefix"`
	Format             string `db:"format" json:"format"`
	Delimiter          string `db:"delimiter" json:"delimiter"`
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

func (s S3) Validate() error {
	if s.Name == "" {
		return fmt.Errorf("source name is required")
	}
	if s.Format != "csv" && s.Format != "json" {
		return fmt.Errorf("invalid format: %s", s.Format)
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
	return nil
}

type BigQuery struct {
	Name            string `db:"name" json:"name"`
	SourceId        string `db:"source_id" json:"source_id"`
	CursorField     string `db:"cursor_field" json:"cursor_field"`
	ProjectId       string `db:"project_id" json:"project_id"`
	DatasetId       string `db:"dataset_id" json:"dataset_id"`
	LastUpdated     string `db:"last_updated" json:"last_updated"`
	CredentialsJson string `json:"credentials_json"`
}

func (s BigQuery) GetSourceName() string {
	return s.Name
}

func (s BigQuery) GetCursorField() string {
	return s.CursorField
}

func (s BigQuery) GetSourceId() string {
	return s.SourceId
}

// BigQuery does not have a default cursor field
func (s BigQuery) GetDefaultCursorField() string {
	return ""
}

func (s BigQuery) SetCursorField() error {
	if s.CursorField != "" {
		return nil
	}
	// TODO: Add link to documentation for BigQuery cursor field
	return fmt.Errorf("BigQuery sources require a cursor field")
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
