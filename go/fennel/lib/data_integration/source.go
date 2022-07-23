package data_integration

import (
	"errors"
	"fmt"
)

var ErrSrcNotFound = errors.New("source not found")
var ErrConnNotFound = errors.New("connector not found")

type Source interface {
	Validate() error
	GetSourceName() string
	Equals(Source) error
}

type S3 struct {
	Name        string `db:"name" json:"name"`
	CursorField string `db:"cursor_field" json:"cursor_field"`
	Bucket      string `db:"bucket" json:"bucket"`
	PathPrefix  string `db:"path_prefix" json:"path_prefix"`
	Format      string `db:"format" json:"format"`
	Delimiter   string `db:"delimiter" json:"delimiter"`
	LastUpdated string `db:"last_updated" json:"last_updated"`
}

func (s S3) GetSourceName() string {
	return s.Name
}

func (s S3) Equals(src Source) error {
	if src.GetSourceName() != s.Name {
		return fmt.Errorf("source name mismatch")
	}
	if s2, ok := src.(S3); ok {
		if s.Bucket == s2.Bucket && s.PathPrefix == s2.PathPrefix && s.Format == s2.Format && s.Delimiter == s2.Delimiter {
			return nil
		}
		fmt.Println(s.Bucket, s.PathPrefix, s.Format, s.Delimiter)
		fmt.Println(s2.Bucket, s2.PathPrefix, s2.Format, s2.Delimiter)
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
	Name        string `db:"name" json:"name"`
	CursorField string `db:"cursor_field" json:"cursor_field"`
	ProjectID   string `db:"project_id" json:"project_id"`
	DatasetID   string `db:"dataset_id" json:"dataset_id"`
	LastUpdated string `db:"last_updated" json:"last_updated"`
}

func (s BigQuery) GetSourceName() string {
	return s.Name
}

func (s BigQuery) Equals(src Source) error {
	if src.GetSourceName() != s.Name {
		return fmt.Errorf("source name mismatch")
	}
	if s2, ok := src.(BigQuery); ok {
		if s.ProjectID == s2.ProjectID && s.DatasetID == s2.DatasetID {
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
	if len(s.ProjectID) == 0 {
		return fmt.Errorf("project_id is required")
	}
	if len(s.DatasetID) == 0 {
		return fmt.Errorf("dataset_id is required")
	}
	return nil
}
