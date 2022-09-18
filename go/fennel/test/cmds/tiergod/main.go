//go:build integration

package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jmoiron/sqlx"

	fkafka "fennel/kafka"
	"fennel/test"
	"fennel/tier"
)

func main() {
	var args tier.TierArgs
	arg.Parse(&args)
	if err := args.Valid(); err != nil {
		panic(err)
	}

	mode := flag.String("mode", "create", "'create' for creating a tier, 'destroy' for destroying, 'prune' for pruning resources in dev dataplane")
	flag.Parse()

	switch *mode {
	case "create":
		if err := test.SetupTier(args); err != nil {
			panic(err)
		}
	case "destroy":
		tier, err := tier.CreateFromArgs(&args)
		if err != nil {
			panic(err)
		}
		if err = test.Teardown(tier); err != nil {
			panic(err)
		}
	case "prune":
		if err := pruneKafka(args.MskKafkaServer, args.MskKafkaUsername, args.MskKafkaPassword, fkafka.SaslScramSha512Mechanism); err != nil {
			panic(err)
		}
		if err := pruneDB(args.MysqlHost, args.MysqlUsername, args.MysqlPassword); err != nil {
			panic(err)
		}
	default:
		panic("invalid mode: valid modes are 'create' or 'destroy'")
	}
}
func pruneKafka(host, username, password, saslMechanism string) (reterr error) {
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password, saslMechanism))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	names := make([]string, 0)
	metadata, err := c.GetMetadata(nil, true, int(time.Second*10))
	for t, _ := range metadata.Topics {
		names = append(names, t)
	}

	fmt.Printf("found %d kafka topics to prune...", len(names))
	defer func() {
		if reterr == nil {
			fmt.Printf("DONE\n")
		}
	}()
	if len(names) > 0 {
		// delete any existing topics of these names
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = c.DeleteTopics(ctx, names)
		return err
	}
	return nil
}

func pruneDB(host, username, password string) error {
	connstr := fmt.Sprintf("%s:%s@tcp(%s)/", username, password, host)
	db, err := sqlx.Open("mysql", connstr)
	if err != nil {
		return fmt.Errorf("could not open DB: %v", err)
	}
	defer db.Close()

	sql := `SELECT schema_name FROM information_schema.schemata WHERE schema_name LIKE 'm\_%' OR schema_name LIKE 't\_%'`
	rows, err := db.Query(sql)
	if err != nil {
		return err
	}
	dbnames := make([]string, 0)
	for rows.Next() {
		var name string
		rows.Scan(&name)
		dbnames = append(dbnames, name)
	}
	fmt.Printf("found %d sql dbnames to prune...", len(dbnames))
	for _, dbname := range dbnames {
		if _, err = db.Exec(fmt.Sprintf("DROP DATABASE %s", dbname)); err != nil {
			return err
		}
	}
	fmt.Printf("DONE\n")
	return nil
}
