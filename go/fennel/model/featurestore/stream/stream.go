package stream

import (
	"context"
	"database/sql"
	"fmt"

	"fennel/featurestore/tier"
	"fennel/lib/featurestore/schema"
	lib "fennel/lib/featurestore/stream"
)

type streamSer struct {
	Name        string `db:"name"`
	Version     uint32 `db:"version"`
	Retention   uint32 `db:"retention"`
	Start       uint32 `db:"start"`
	Schema      []byte `db:"stream_schema"`
	LastUpdated string `db:"last_updated"`
}

func StoreStream(ctx context.Context, tier tier.Tier, strm lib.Stream) error {
	strmSer, err := toStreamSer(strm)
	if err != nil {
		return fmt.Errorf("failed to serialize stream: %s", err)
	}
	sql := "INSERT INTO stream (name, version, retention, start, stream_schema) VALUES (?, ?, ?, ?, ?)"
	_, err = tier.DB.QueryContext(ctx, sql, strm.Name, strm.Version, strm.Retention, strm.Start, strmSer.Schema)
	if err != nil {
		return fmt.Errorf("failed to store source: %w", err)
	}
	return nil
}

func RetrieveStream(ctx context.Context, tier tier.Tier, name string) (lib.Stream, error) {
	var strmSer streamSer
	sqlStr := "SELECT name, version, retention, start, stream_schema FROM stream WHERE name = ?"
	err := tier.DB.GetContext(ctx, &strmSer, sqlStr, name)
	if err != nil && err == sql.ErrNoRows {
		return lib.Stream{}, lib.ErrStreamNotFound
	}
	if err != nil {
		return lib.Stream{}, err
	}
	strm, err := fromStreamSer(strmSer)
	if err != nil {
		return lib.Stream{}, fmt.Errorf("failed to deserialize stream: %s", err)
	}
	return strm, nil
}

func DeleteStream(ctx context.Context, tier tier.Tier, name string) error {
	_, err := tier.DB.ExecContext(ctx, "DELETE FROM stream WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("failed to delete source: %w", err)
	}
	return nil
}

func toStreamSer(strm lib.Stream) (streamSer, error) {
	schSer, err := schema.Serialize(strm.Schema)
	if err != nil {
		return streamSer{}, err
	}
	return streamSer{
		Name:      strm.Name,
		Version:   strm.Version,
		Retention: strm.Retention,
		Start:     strm.Start,
		Schema:    schSer,
	}, nil
}

func fromStreamSer(strmSer streamSer) (lib.Stream, error) {
	sch, err := schema.Deserialize(strmSer.Schema)
	if err != nil {
		return lib.Stream{}, err
	}
	return lib.Stream{
		Name:      strmSer.Name,
		Version:   strmSer.Version,
		Retention: strmSer.Retention,
		Start:     strmSer.Start,
		Schema:    sch,
	}, nil
}
