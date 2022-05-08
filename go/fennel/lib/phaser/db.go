package phaser

import (
	"context"
	"fennel/tier"
	"fmt"
	"strconv"
	"strings"
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

func getPhaser(p PhaserSer) Phaser {
	var p2 Phaser
	p2.Namespace = p.Namespace
	p2.Identifier = p.Identifier
	p2.S3Bucket = p.S3Bucket
	p2.S3Prefix = p.S3Prefix
	p2.Schema, _ = FromPhaserSchema(p.Schema)
	p2.TTL = time.Duration(p.TTL) * time.Second
	p2.UpdateVersion = p.UpdateVersion
	return p2
}

func RetrieveAll(ctx context.Context, tier tier.Tier) ([]Phaser, error) {
	ret := make([]PhaserSer, 0)
	err := tier.DB.SelectContext(ctx, &ret, `SELECT * FROM phaser`)
	if err != nil {
		return nil, err
	}
	phasers := make([]Phaser, 0, len(ret))
	for _, pSer := range ret {
		phasers = append(phasers, getPhaser(pSer))
	}
	return phasers, nil
}

func DelPhaser(ctx context.Context, tier tier.Tier, namespace, identifier string) error {
	_, err := tier.DB.ExecContext(ctx, `DELETE FROM phaser WHERE namespace = ? AND identifier = ?`, namespace, identifier)
	return err
}

func RetrievePhaser(ctx context.Context, tier tier.Tier, namespace, identifier string) (Phaser, error) {
	var p PhaserSer
	err := tier.DB.GetContext(ctx, &p, `SELECT * FROM phaser WHERE namespace = ? AND identifier = ? LIMIT 1`, namespace, identifier)
	if err != nil {
		return Phaser{}, err
	}
	return getPhaser(p), nil
}

func RetrievePhasers(ctx context.Context, tier tier.Tier, namespace, identifier []string) ([]Phaser, error) {
	if len(identifier) != len(namespace) {
		return []Phaser{}, fmt.Errorf("identifier and namespace must be the same length")
	}

	// Dedupe all namespaces and identifiers
	namespaceIdentifiers := make(map[string]Phaser)
	for i := 0; i < len(identifier); i++ {
		namespaceIdentifiers[namespace[i]+":"+identifier[i]] = Phaser{}
	}

	sql := `
		SELECT *
		FROM phaser
		WHERE (namespace, identifier) in 
	`
	v := make([]interface{}, 0, len(namespaceIdentifiers))
	inval := "("
	for key, _ := range namespaceIdentifiers {
		split := strings.Split(key, ":")
		inval += "(?, ?),"
		v = append(v, split[0], split[1])
	}
	inval = strings.TrimSuffix(inval, ",") // remove the last trailing comma
	inval += ")"
	sql += inval
	phaserReqs := make([]PhaserSer, 0)
	err := tier.DB.SelectContext(ctx, &phaserReqs, sql, v...)
	if err != nil {
		return []Phaser{}, err
	}
	if len(phaserReqs) == 0 {
		return []Phaser{}, PhaserNotFound
	}
	for _, p := range phaserReqs {
		namespaceIdentifiers[p.Namespace+":"+p.Identifier] = getPhaser(p)
	}
	phasers := make([]Phaser, 0, len(namespace))
	for i := 0; i < len(namespace); i++ {
		phasers = append(phasers, namespaceIdentifiers[namespace[i]+":"+identifier[i]])
	}

	return phasers, nil
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
