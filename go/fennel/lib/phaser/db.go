package phaser

import (
	"context"
	"fennel/tier"
	"fmt"
	"strconv"
	"time"
)

type PhaserSer struct {
	Namespace     string `db:"namespace"`
	Identifier    string `db:"identifier"`
	S3Bucket      string `db:"s3_bucket"`
	S3Prefix      string `db:"s3_prefix"`
	Schema        string `db:"phaser_schema"`
	UpdateVersion uint64 `db:"update_version"`
	TTL           uint64 `db:"ttl"`
}

func RetrieveAll(ctx context.Context, tier tier.Tier) ([]Phaser, error) {
	ret := make([]PhaserSer, 0)
	err := tier.DB.SelectContext(ctx, &ret, `SELECT * FROM phaser`)
	if err != nil {
		return nil, err
	}
	phasers := make([]Phaser, 0, len(ret))
	for _, pSer := range ret {
		var p Phaser
		p.Namespace = pSer.Namespace
		p.Identifier = pSer.Identifier
		p.S3Bucket = pSer.S3Bucket
		p.S3Prefix = pSer.S3Prefix
		p.Schema, err = FromPhaserSchema(pSer.Schema)
		p.TTL = time.Duration(pSer.TTL) * time.Second
		p.UpdateVersion = pSer.UpdateVersion
		phasers = append(phasers, p)
	}
	return phasers, nil
}

func RetrievePhaser(ctx context.Context, tier tier.Tier, namespace, identifier string) (Phaser, error) {
	var p PhaserSer
	err := tier.DB.GetContext(ctx, &p, `SELECT * FROM phaser WHERE namespace = ? AND identifier = ? LIMIT 1`, namespace, identifier)
	if err != nil {
		return Phaser{}, err
	}
	var p2 Phaser
	p2.Namespace = p.Namespace
	p2.Identifier = p.Identifier
	p2.S3Bucket = p.S3Bucket
	p2.S3Prefix = p.S3Prefix
	p2.Schema, err = FromPhaserSchema(p.Schema)
	p2.TTL = time.Duration(p.TTL) * time.Second
	p2.UpdateVersion = p.UpdateVersion
	return p2, nil
}

func GetLatestUpdatedVersion(ctx context.Context, tier tier.Tier, namespace, identifier string) (uint64, error) {
	var value [][]byte = nil
	err := tier.DB.SelectContext(ctx, &value, `SELECT update_version FROM phaser WHERE namespace = ? AND identifier = ? LIMIT 1`, namespace, identifier)
	if err != nil {
		return 0, err
	} else if len(value) == 0 {
		return 0, PhaserNotFound
	}
	return strconv.ParseUint(string(value[0]), 10, 64)
}

func InitializePhaser(ctx context.Context, tier tier.Tier, s3Bucket, s3Prefix, namespace, identifier string, ttl time.Duration, schema PhaserSchema) error {
	if len(identifier) > 255 {
		return fmt.Errorf("identifier name can not be longer than 255 chars")
	}
	schemaStr, err := schema.String()
	if err != nil {
		return err
	}
	_, err = tier.DB.ExecContext(ctx, `INSERT INTO phaser (namespace, identifier, s3_bucket,  s3_prefix, phaser_schema, update_version, ttl) VALUES (?, ?, ?, ?, ?, ?, ?)`, namespace, identifier, s3Bucket, s3Prefix, schemaStr, 0, int(ttl.Seconds()))
	return err
}

func UpdateVersion(ctx context.Context, tier tier.Tier, namespace, identifier string, update_version uint64) error {
	_, err := tier.DB.ExecContext(ctx, `UPDATE phaser SET update_version = ? WHERE namespace = ? AND identifier = ?`, update_version, namespace, identifier)
	return err
}

type notFound int

func (_ notFound) Error() string {
	return "phaser not found"
}

var PhaserNotFound = notFound(1)
var _ error = PhaserNotFound
