package nitrous

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"fennel/lib/instancemetadata"
	"fennel/lib/timer"

	libkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/nitrous/backup"
	"fennel/resource"
	"fennel/s3"

	"github.com/raulk/clock"
	"github.com/samber/mo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type NitrousArgs struct {
	s3.S3Args        `json:"s3_._s3_args"`
	PlaneID          ftypes.RealmID `arg:"--plane-id,env:PLANE_ID" json:"plane_id,omitempty"`
	MskKafkaServer   string         `arg:"--msk-kafka-server,env:MSK_KAFKA_SERVER_ADDRESS" json:"msk_kafka_server,omitempty"`
	MskKafkaUsername string         `arg:"--msk-kafka-user,env:MSK_KAFKA_USERNAME" json:"msk_kafka_username,omitempty"`
	MskKafkaPassword string         `arg:"--msk-kafka-password,env:MSK_KAFKA_PASSWORD" json:"msk_kafka_password,omitempty"`
	BadgerDir        string         `arg:"--badger_dir,env:BADGER_DIR" json:"badger_dir,omitempty"`
	PebbleDir        string         `arg:"--pebble_dir,env:PEBBLE_DIR" json:"pebble_dir,omitempty"`
	GravelDir        string         `arg:"--gravel_dir,env:GRAVEL_DIR" json:"gravel_dir,omitempty"`
	Partitions       []int32        `arg:"--partitions,env:PARTITIONS" json:"partitions,omitempty"`
	BinPartitions    uint32         `arg:"--binlog_partitions,env:BINLOG_PARTITIONS" json:"bin_partitions,omitempty"`

	InstanceMetadataServiceAddr string `arg:"--instance-metadata-service-addr,env:INSTANCE_METADATA_SERVICE_ADDR" json:"instance_metadata_service_Addr,omitempty"`

	// Identity should be unique for each instance of nitrous. The IDENTITY environment
	// variable should be unique for each replica of a StatefulSet in k8s.
	Identity string `arg:"--identity,env:IDENTITY" json:"identity" default:"localhost"`
	// Flag to enable data compression.
	Compress            bool          `arg:"--compress,env:COMPRESS" json:"compress" default:"false"`
	Dev                 bool          `arg:"--dev" default:"true" json:"dev,omitempty"`
	BackupNode          bool          `arg:"--backup-node,env:BACKUP_NODE" json:"backup_node,omitempty"`
	BackupBucket        string        `arg:"--backup-bucket,env:BACKUP_BUCKET" json:"backup_bucket,omitempty"`
	RemoteBackupsToKeep uint32        `arg:"--remote-backups-to-keep,env:REMOTE_BACKUPS_TO_KEEP" default:"2" json:"remote_backups_to_keep,omitempty"`
	BackupFrequency     time.Duration `arg:"--backup-frequency,env:BACKUP_FREQUENCY" json:"backup_frequency,omitempty"`
	ForceLoadFromBackup bool          `arg:"--force-load-from-backup,env:FORCE_LOAD_FROM_BACKUP" json:"force_load_from_backup,omitempty"`
	ShardName           string        `arg:"--shard-name,env:SHARD_NAME" default:"default" json:"shard_name,omitempty"`
}

func (args NitrousArgs) Valid() error {
	// TODO: implement
	return nil
}

type KafkaConsumerFactory func(libkafka.ConsumerConfig) (libkafka.FConsumer, error)

type Nitrous struct {
	PlaneID              ftypes.RealmID
	Identity             string
	Clock                clock.Clock
	Partitions           []int32
	BinlogPartitions     uint32
	DbDir                string
	KafkaConsumerFactory KafkaConsumerFactory
	backupManager        *backup.BackupManager
}

func purgeOldData(dir string) {
	// Remove other DB directories and only keep the one that is actively being used
	items, err := ioutil.ReadDir(dir)
	if err != nil {
		// not super critical, and shouldn't block the server starts
		zap.L().Error("Failed to open the dir when trying to purge data", zap.String("dir", dir), zap.Error(err))
		return
	}
	for _, item := range items {
		// now going to erase this folder
		fullName := filepath.Join(dir, item.Name())
		err := os.RemoveAll(fullName)
		if err == nil {
			zap.L().Info("Successfully purged previous directory", zap.String("dirItem", fullName))
		} else {
			zap.L().Error("Failed to purge previous directory", zap.String("dirItem", fullName), zap.Error(err))
		}
	}
}

func (n *Nitrous) Backup() error {
	ctx := context.Background()
	ctx, t := timer.Start(ctx, n.PlaneID, "nitrous.Backup")
	defer t.Stop()
	return n.backupManager.BackupPath(ctx, n.DbDir, time.Now().Format(time.RFC3339))
}

func (n *Nitrous) PurgeOldBackups() {
	ctx := context.Background()
	ctx, t := timer.Start(ctx, n.PlaneID, "nitrous.PurgeOldBackups")
	defer t.Stop()

	n.backupManager.PurgeOldBackups(ctx)
}

// restoreBackupOrReuseData looks for the mentioned DB directory locally, if the directory is empty, it tries to
// restore a backup. Otherwise continues using the local data
func restoreBackupOrReuseData(bm *backup.BackupManager, args NitrousArgs) error {
	ctx := context.Background()
	dbDir := args.GravelDir
	err := os.MkdirAll(dbDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create new directory for DB: %s, err: %v", dbDir, err)
	}

	// check if the local files exist, if so load from it always
	dirEmpty, err := backup.DirIsEmpty(dbDir)
	if err != nil {
		// this should never happen
		return fmt.Errorf("failed to check if the directory: %s is empty, %w", dbDir, err)
	}

	// if nitrous is forced to load from the backup, load from it always
	if dirEmpty || args.ForceLoadFromBackup {
		// clean up the directory
		purgeOldData(dbDir)

		// now restore to it
		err := bm.RestoreLatest(ctx, dbDir)
		if err != nil {
			return fmt.Errorf("failed to restore latest backup to directory: %s, err: %w", dbDir, err)
		}
	} else {
		zap.L().Info("reusing the existing data in the directory", zap.String("dir", dbDir))
	}
	return nil
}

func CreateFromArgs(args NitrousArgs) (Nitrous, error) {
	scope := resource.NewPlaneScope(args.PlaneID)

	log.Print("Creating logger")
	var logger *zap.Logger
	var err error
	if args.Dev {
		logger, err = zap.NewDevelopment()
	} else {
		config := zap.NewProductionConfig()
		config.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
		logger, err = config.Build(
			zap.AddCaller(),
			zap.AddStacktrace(zap.ErrorLevel),
		)
	}

	if len(args.Partitions) == 0 {
		zap.L().Info("no binlog partitions assigned, defaults to ALL")
	} else {
		zap.L().Info("binlog partitions assigned", zap.Int32s("partitions", args.Partitions))
	}

	azId := mo.None[string]()
	if len(args.InstanceMetadataServiceAddr) > 0 {
		id, err := instancemetadata.GetAvailabilityZoneId(args.InstanceMetadataServiceAddr)
		if err != nil {
			return Nitrous{}, fmt.Errorf("failed to get AZ Id: %v", err)
		}
		azId = mo.Some(id)
	}

	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to construct logger: %w", err)
	}
	logger = logger.With(
		zap.Uint32("plane", args.PlaneID.Value()),
		zap.String("identity", args.Identity),
	)
	_ = zap.ReplaceGlobals(logger)

	// Initialize kafka consumer factory.
	consumerFactory := func(config libkafka.ConsumerConfig) (libkafka.FConsumer, error) {
		kafkaConsumerConfig := libkafka.RemoteConsumerConfig{
			BootstrapServer: args.MskKafkaServer,
			Username:        args.MskKafkaUsername,
			Password:        args.MskKafkaPassword,
			SaslMechanism:   libkafka.SaslScramSha512Mechanism,
			ConsumerConfig:  config,
			AzId:            azId,
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		return kafkaConsumer.(libkafka.FConsumer), nil
	}

	s3Store, err := backup.NewS3Store(args.Region, args.BackupBucket, args.ShardName, scope.ID())
	if err != nil {
		zap.L().Error("failed to create the s3store for backup manager", zap.Error(err))
		return Nitrous{}, err
	}

	bm, err := backup.NewBackupManager(args.PlaneID, s3Store, int(args.RemoteBackupsToKeep))
	if err != nil {
		zap.L().Error("failed to create backup manager", zap.Error(err))
		return Nitrous{}, err
	}

	err = restoreBackupOrReuseData(bm, args)
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create db after all the tries: %v", err)
	}

	return Nitrous{
		PlaneID:              scope.ID(),
		Identity:             args.Identity,
		KafkaConsumerFactory: consumerFactory,
		Clock:                clock.New(),
		Partitions:           args.Partitions,
		BinlogPartitions:     args.BinPartitions,
		DbDir:                args.GravelDir,
		backupManager:        bm,
	}, nil
}
