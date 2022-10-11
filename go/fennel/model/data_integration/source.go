package data_integration

import (
	"context"
	"database/sql"
	"fennel/lib/data_integration"
	"fennel/tier"
	"fmt"
	"reflect"
)

type sourceSer struct {
	Name        string `db:"name"`
	Type        string `db:"type"`
	SourceId    string `db:"source_id"`
	CursorField string `db:"cursor_field"`
	LastUpdated string `db:"last_updated"`
}

func StoreSource(ctx context.Context, tier tier.Tier, src data_integration.Source, srcId string) error {
	sql := "INSERT INTO source (name, type, source_id ) VALUES (?, ?, ?)"
	_, err := tier.DB.QueryContext(ctx, sql, src.GetSourceName(), reflect.TypeOf(src).Name(), srcId)
	if err != nil {
		return fmt.Errorf("failed to store source: %w", err)
	}

	switch srcDerived := src.(type) {
	case data_integration.S3:
		sql := "INSERT INTO s3_source (name, bucket, path_prefix, format, delimiter, json_schema, source_id) VALUES (?, ?, ?, ?, ?, ?, ?)"
		_, err = tier.DB.QueryContext(ctx, sql, srcDerived.Name, srcDerived.Bucket, srcDerived.PathPrefix, srcDerived.Format, srcDerived.Delimiter, srcDerived.Schema, srcId)
	case data_integration.BigQuery:
		sql := `INSERT INTO bigquery_source (name,  project_id, dataset_id, source_id) VALUES (?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql, srcDerived.Name, srcDerived.ProjectId, srcDerived.DatasetId, srcId)
	case data_integration.Postgres:
		sql := `INSERT INTO postgres_source (name, host, port, db_name, jdbc_params, source_id) VALUES (?, ?, ?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql, srcDerived.Name, srcDerived.Host, srcDerived.Port, srcDerived.Dbname, srcDerived.JdbcParams, srcId)
	case data_integration.MySQL:
		sql := `INSERT INTO mysql_source (name, host, port, db_name, jdbc_params, source_id) VALUES (?, ?, ?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql, srcDerived.Name, srcDerived.Host, srcDerived.Port, srcDerived.Dbname, srcDerived.JdbcParams, srcId)
	case data_integration.Snowflake:
		sql := `INSERT INTO snowflake_source (name, host, warehouse, role, db_name, jdbc_params, db_schema, source_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
		_, err = tier.DB.QueryContext(ctx, sql, srcDerived.Name, srcDerived.Host, srcDerived.Warehouse, srcDerived.Role, srcDerived.Dbname, srcDerived.JdbcParams, srcDerived.Schema, srcId)
	default:
		err = fmt.Errorf("unsupported source type: %T found during storing source", src)
	}
	return err
}

func RetrieveSource(ctx context.Context, tier tier.Tier, srcName string) (data_integration.Source, error) {
	var srcSer sourceSer
	err := tier.DB.GetContext(ctx, &srcSer, "SELECT * FROM source WHERE name = ?", srcName)
	if err != nil && err == sql.ErrNoRows {
		return nil, data_integration.ErrSrcNotFound
	}
	if err != nil {
		return nil, err
	}
	switch srcSer.Type {
	case "S3":
		var src data_integration.S3
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM s3_source WHERE name = ?", srcName)
		return src, err
	case "BigQuery":
		var src data_integration.BigQuery
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM bigquery_source WHERE name = ?", srcName)
		return src, err
	case "Postgres":
		var src data_integration.Postgres
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM postgres_source WHERE name = ?", srcName)
		return src, err
	case "MySQL":
		var src data_integration.MySQL
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM mysql_source WHERE name = ?", srcName)
		return src, err
	case "Snowflake":
		var src data_integration.Snowflake
		err = tier.DB.GetContext(ctx, &src, "SELECT * FROM snowflake_source WHERE name = ?", srcName)
		return src, err
	default:
		return nil, fmt.Errorf("unsupported source type: %s found during retrieving source", srcSer.Type)
	}
}

func DeleteSource(ctx context.Context, tier tier.Tier, src data_integration.Source) error {
	_, err := tier.DB.ExecContext(ctx, "DELETE FROM source WHERE name = ?", src.GetSourceName())
	if err != nil {
		return fmt.Errorf("failed to delete source: %w", err)
	}
	switch srcDerived := src.(type) {
	case data_integration.S3:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM s3_source WHERE name = ?", srcDerived.Name)
	case data_integration.BigQuery:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM bigquery_source WHERE name = ?", srcDerived.Name)
	case data_integration.Postgres:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM postgres_source WHERE name = ?", srcDerived.Name)
	case data_integration.MySQL:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM mysql_source WHERE name = ?", srcDerived.Name)
	case data_integration.Snowflake:
		_, err = tier.DB.ExecContext(ctx, "DELETE FROM snowflake_source WHERE name = ?", srcDerived.Name)
	default:
		err = fmt.Errorf("unsupported source type: %T found during deleting source", srcDerived)
	}
	return err
}
