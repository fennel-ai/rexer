package stream

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"fennel/featurestore/tier"
	lib "fennel/lib/featurestore/stream"
)

type sourceSer struct {
	Name        string `db:"name"`
	Type        string `db:"type"`
	SourceId    string `db:"source_id"`
	CursorField string `db:"cursor_field"`
	LastUpdated string `db:"last_updated"`
}

func StoreSource(ctx context.Context, tier tier.Tier, src lib.Source, srcId string) error {
	sql := "INSERT INTO source (name, type, source_id ) VALUES (?, ?, ?)"
	_, err := tier.DB.QueryContext(ctx, sql, src.GetSourceName(), reflect.TypeOf(src).Name(), srcId)
	if err != nil {
		return fmt.Errorf("failed to store source: %w", err)
	}

	switch srcDerived := src.(type) {
	case lib.S3:
		sql := `INSERT INTO s3_source
            (name, bucket, path_prefix, format, delimiter, json_schema, source_id) VALUES (?, ?, ?, ?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql,
			srcDerived.Name, srcDerived.Bucket, srcDerived.PathPrefix,
			srcDerived.Format, srcDerived.Delimiter, srcDerived.Schema, srcId,
		)
	case lib.BigQuery:
		sql := `INSERT INTO bigquery_source (name,  project_id, dataset_id, source_id) VALUES (?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql, srcDerived.Name, srcDerived.ProjectId, srcDerived.DatasetId, srcId)
	case lib.Postgres:
		sql := `INSERT INTO postgres_source
            (name, host, port, db_name, jdbc_params, source_id) VALUES (?, ?, ?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql,
			srcDerived.Name, srcDerived.Host, srcDerived.Port, srcDerived.Dbname, srcDerived.JdbcParams, srcId,
		)
	case lib.MySQL:
		sql := `INSERT INTO mysql_source (name, host, port, db_name, jdbc_params, source_id) VALUES (?, ?, ?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql,
			srcDerived.Name, srcDerived.Host, srcDerived.Port, srcDerived.Dbname, srcDerived.JdbcParams, srcId,
		)
	default:
		err = fmt.Errorf("unsupported source type: %T found during storing source", src)
	}
	return err
}

func RetrieveSource(ctx context.Context, tier tier.Tier, srcName string) (lib.Source, error) {
	var srcSer sourceSer
	err := tier.DB.GetContext(ctx, &srcSer, "SELECT * FROM source WHERE name = ?", srcName)
	if err != nil && err == sql.ErrNoRows {
		return nil, lib.ErrSrcNotFound
	}
	if err != nil {
		return nil, err
	}
	switch srcSer.Type {
	case "S3":
		var src lib.S3
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM s3_source WHERE name = ?", srcName)
		return src, err
	case "BigQuery":
		var src lib.BigQuery
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM bigquery_source WHERE name = ?", srcName)
		return src, err
	case "Postgres":
		var src lib.Postgres
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM postgres_source WHERE name = ?", srcName)
		return src, err
	case "MySQL":
		var src lib.MySQL
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM mysql_source WHERE name = ?", srcName)
		return src, err
	default:
		return nil, fmt.Errorf("unsupported source type: %s found during retrieving source", srcSer.Type)
	}
}

func DeleteSource(ctx context.Context, tier tier.Tier, src lib.Source) error {
	_, err := tier.DB.ExecContext(ctx, "DELETE FROM source WHERE name = ?", src.GetSourceName())
	if err != nil {
		return fmt.Errorf("failed to delete source: %w", err)
	}
	switch srcDerived := src.(type) {
	case lib.S3:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM s3_source WHERE name = ?", srcDerived.Name)
	case lib.BigQuery:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM bigquery_source WHERE name = ?", srcDerived.Name)
	case lib.Postgres:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM postgres_source WHERE name = ?", srcDerived.Name)
	case lib.MySQL:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM mysql_source WHERE name = ?", srcDerived.Name)
	default:
		err = fmt.Errorf("unsupported source type: %T found during deleting source", srcDerived)
	}
	return err
}
