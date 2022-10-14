package stream

import (
	"fmt"

	"fennel/lib/data_integration"
	"fennel/lib/featurestore/stream"
)

func toDataIntegrationConnector(conn stream.Connector) data_integration.Connector {
	return data_integration.Connector{
		Name:        conn.Name,
		StreamName:  conn.StreamName,
		CursorField: conn.CursorField,
		ConnId:      conn.ConnId,
	}
}

func toDataIntegrationSource(src stream.Source) (data_integration.Source, error) {
	switch src := src.(type) {
	case stream.S3:
		return data_integration.S3{
			Name:               src.Name,
			SourceId:           src.SourceId,
			Bucket:             src.Bucket,
			PathPrefix:         src.PathPrefix,
			Format:             src.Format,
			Delimiter:          src.Delimiter,
			Schema:             src.Schema,
			AWSAccessKeyId:     src.AWSAccessKeyId,
			AWSSecretAccessKey: src.AWSSecretAccessKey,
		}, nil
	case stream.BigQuery:
		return data_integration.BigQuery{
			Name:            src.Name,
			SourceId:        src.SourceId,
			ProjectId:       src.ProjectId,
			DatasetId:       src.DatasetId,
			CredentialsJson: src.CredentialsJson,
		}, nil
	case stream.Postgres:
		return data_integration.Postgres{SQLSource: data_integration.SQLSource{
			Name:     src.Name,
			SourceId: src.SourceId,
			Host:     src.Host,
			Dbname:   src.Dbname,
			Username: src.Username,
			Password: src.Password,
			Port:     src.Port,
		}}, nil
	case stream.MySQL:
		return data_integration.MySQL{SQLSource: data_integration.SQLSource{
			Name:       src.Name,
			SourceId:   src.SourceId,
			Host:       src.Host,
			Dbname:     src.Dbname,
			Username:   src.Username,
			Password:   src.Password,
			JdbcParams: src.JdbcParams,
			Port:       src.Port,
		}}, nil
	default:
		return nil, fmt.Errorf("unsupported source type")
	}
}
