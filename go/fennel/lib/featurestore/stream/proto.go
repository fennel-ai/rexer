package stream

import (
	"fmt"

	"fennel/lib/featurestore/stream/proto"
)

func SourceFromRequest(req *proto.CreateSourceRequest) (Source, error) {
	var src Source

	switch req.Source.(type) {
	case *proto.CreateSourceRequest_S3:
		reqS3 := req.GetS3()
		src = S3{
			Name:               req.Name,
			Bucket:             reqS3.Bucket,
			PathPrefix:         reqS3.PathPrefix,
			Format:             "csv",
			Delimiter:          ",",
			Schema:             reqS3.Schema,
			AWSAccessKeyId:     reqS3.AwsAccessKeyId,
			AWSSecretAccessKey: reqS3.AwsSecretAccessKey,
		}
	case *proto.CreateSourceRequest_Bigquery:
		reqBQ := req.GetBigquery()
		src = BigQuery{
			Name:            req.Name,
			ProjectId:       reqBQ.ProjectId,
			DatasetId:       reqBQ.Dataset,
			CredentialsJson: reqBQ.CredentialsJson,
		}
	case *proto.CreateSourceRequest_Sql:
		reqSQL := req.GetSql()
		sqlSrc := SQL{
			Name:       req.Name,
			Host:       reqSQL.Host,
			Dbname:     reqSQL.Db,
			Username:   reqSQL.Username,
			Password:   reqSQL.Password,
			JdbcParams: reqSQL.JdbcParams,
			Port:       int(reqSQL.Port),
		}
		switch reqSQL.SqlType {
		case proto.SQL_Postgres:
			src = Postgres{sqlSrc}
		case proto.SQL_MySQL:
			src = MySQL{sqlSrc}
		default:
			return nil, fmt.Errorf("unknown sql type")
		}
	default:
		return nil, fmt.Errorf("unknown source type")
	}
	return src, nil
}

func ConnectorFromRequest(req *proto.CreateConnectorRequest, streamName string) Connector {
	return Connector{
		Name:        req.Name,
		SourceName:  req.SourceName,
		SourceType:  req.SourceType,
		StreamName:  streamName,
		Function:    req.ConnectorFunction,
		CursorField: req.CursorField,
		TableName:   req.TableName,
	}
}
