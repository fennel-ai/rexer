package nitrous

import (
	"fennel/lib/instancemetadata"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
	"time"

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
	s3.S3Args          `json:"s3_._s3_args"`
	PlaneID            ftypes.RealmID `arg:"--plane-id,env:PLANE_ID" json:"plane_id,omitempty"`
	MskKafkaServer     string         `arg:"--msk-kafka-server,env:MSK_KAFKA_SERVER_ADDRESS" json:"msk_kafka_server,omitempty"`
	MskKafkaUsername   string         `arg:"--msk-kafka-user,env:MSK_KAFKA_USERNAME" json:"msk_kafka_username,omitempty"`
	MskKafkaPassword   string         `arg:"--msk-kafka-password,env:MSK_KAFKA_PASSWORD" json:"msk_kafka_password,omitempty"`
	BadgerDir          string         `arg:"--badger_dir,env:BADGER_DIR" json:"badger_dir,omitempty"`
	PebbleDir          string         `arg:"--pebble_dir,env:PEBBLE_DIR" json:"pebble_dir,omitempty"`
	GravelDir          string         `arg:"--gravel_dir,env:GRAVEL_DIR" json:"gravel_dir,omitempty"`
	Partitions 		   []int32 	  	  `arg:"--partitions,env:PARTITIONS" json:"partitions,omitempty"`
	BinPartitions 	   uint32 		  `arg:"--binlog_partitions,env:BINLOG_PARTITIONS" json:"bin_partitions,omitempty"`
	BadgerBlockCacheMB int64          `arg:"--badger_block_cache_mb,env:BADGER_BLOCK_CACHE_MB" json:"badger_block_cache_mb,omitempty"`
	RistrettoMaxCost   uint64         `arg:"--ristretto_max_cost,env:RISTRETTO_MAX_COST" json:"ristretto_max_cost,omitempty"`
	RistrettoAvgCost   uint64         `arg:"--ristretto_avg_cost,env:RISTRETTO_AVG_COST" json:"ristretto_avg_cost,omitempty" default:"1000"`

	InstanceMetadataServiceAddr string `arg:"--instance-metadata-service-addr,env:INSTANCE_METADATA_SERVICE_ADDR" json:"instance_metadata_service_Addr,omitempty"`

	// Identity should be unique for each instance of nitrous. The IDENTITY environment
	// variable should be unique for each replica of a StatefulSet in k8s.
	Identity string `arg:"--identity,env:IDENTITY" json:"identity" default:"localhost"`
	// Flag to enable data compression.
	Compress bool `arg:"--compress,env:COMPRESS" json:"compress" default:"false"`
	Dev      bool `arg:"--dev" default:"true" json:"dev,omitempty"`
	BackupNode   bool   `arg:"--backupnode" json:"backup_node,omitempty"`
	BackupBucket string `arg:"--backup-bucket,env:BACKUP_BUCKET" json:"backup_bucket,omitempty"`
	BackupFrequency time.Duration `arg:"--backup-frequency,env:BACKUP_FREQUENCY" json:"backup_frequency,omitempty"`
	LocalCopyStalenessDuration time.Duration `arg:"--local-copy-staleness-duration,env:LOCAL_COPY_STALENESS_DURATION" json:"local_copy_staleness_duration,omitempty"`
	ShardName    string `arg:"--shard-name,env:SHARD_NAME" default:"default" json:"shard_name,omitempty"`
}

func (args NitrousArgs) Valid() error {
	// TODO: implement
	return nil
}

type KafkaConsumerFactory func(libkafka.ConsumerConfig) (libkafka.FConsumer, error)

const dataDirPrefix string = "badgerdb-data-"

type Nitrous struct {
	PlaneID              ftypes.RealmID
	Identity             string
	Clock                clock.Clock
	Partitions 			 []int32
	BinlogPartitions 	 uint32
	DbDir				 string
	KafkaConsumerFactory KafkaConsumerFactory
	backupManager        *backup.BackupManager
}

func writeFlag(flagFileName string, value string) error {
	// store a piece of information into a file
	fFlag, err := os.Create(flagFileName)
	if err != nil {
		return err
	}
	_, err = fFlag.WriteString(value)
	if err != nil {
		return err
	}
	err = fFlag.Close()
	if err != nil {
		return err
	}
	return nil
}

func readFlag(flagFileName string) string {
	content, err := ioutil.ReadFile(flagFileName)
	if err != nil {
		return ""
	}
	return string(content)
}

func purgeOldData(rootDir string, dirToKeep string) {
	// Remove other DB directories and only keep the one that is actively being used
	items, err := ioutil.ReadDir(rootDir)
	if err != nil {
		// not super critical, and shouldn't block the server starts
		zap.L().Error("Failed to open the root dir when trying to purge other copies of data", zap.String("dir", rootDir), zap.Error(err))
		return
	}
	for _, item := range items {
		if item.IsDir() {
			if strings.HasPrefix(item.Name(), dataDirPrefix) {
				// interesting folder
				if strings.HasSuffix(dirToKeep, item.Name()) {
					continue // keep this folder
				}
				// now going to erase this folder
				fullName := rootDir + "/" + item.Name()
				err := os.RemoveAll(fullName)
				if err == nil {
					zap.L().Info("Successfully purged previous directory", zap.String("directory", fullName))
				} else {
					zap.L().Error("Failed to purge previous directory", zap.String("directory", fullName), zap.Error(err))
				}
			}
		}
	}
}

func getDirChangeTime(dir string) int64 {
	fileInfo, err := os.Stat(dir)
	if err != nil {
		zap.L().Error("Failed to get the change time of the directory", zap.String("dir", dir), zap.Error(err))
		return 0
	}
	return fileInfo.ModTime().Unix()
}

func purgeOldBackups(bm *backup.BackupManager) {
	const backupToKeep = 5
	backupList, err := bm.ListBackups()
	if err != nil {
		zap.L().Error("Failed to list backup while purging old backups", zap.Error(err))
		return
	}
	sort.Strings(backupList)
	zap.L().Info("Backups to keep", zap.Strings("list_of_versions", backupList))
	if len(backupList) < backupToKeep {
		return
	}
	err = bm.BackupCleanup(backupList[len(backupList)-backupToKeep:])
	if err != nil {
		zap.L().Info("Failed to purge old backups", zap.Error(err))
	}
}

func (n *Nitrous) Backup(args NitrousArgs) error {
	currentDirFlag := args.GravelDir + "/current_data_folder.txt"
	currentDBDir := readFlag(currentDirFlag)
	return n.backupManager.BackupPath(currentDBDir, time.Now().Format(time.RFC3339))
}

func dbDir(bm *backup.BackupManager, args NitrousArgs, scope resource.Scope) (string, error) {
	currentDirFlag := args.GravelDir + "/current_data_folder.txt"

	newRestoreDir := fmt.Sprintf("%s/%s%d", args.GravelDir, dataDirPrefix, time.Now().Unix())
	err := os.Mkdir(newRestoreDir, os.ModePerm)
	if err != nil {
		return "", fmt.Errorf("failed to create new directory for restoring DB: %s, err: %v", newRestoreDir, err)
	}

	// Initialize layered storage.
	currentDBDir := readFlag(currentDirFlag)
	lastWriteTsSecs := getDirChangeTime(currentDBDir)
	var DBDirPrecedence []string

	if lastWriteTsSecs + int64(args.LocalCopyStalenessDuration.Seconds()) >= time.Now().Unix() {
		// pull from the last DB
		zap.L().Info("Found previous last write timestamp, going to reload the previous DB first", zap.Int64("timestamp", lastWriteTsSecs))
		DBDirPrecedence = append(DBDirPrecedence, currentDBDir, newRestoreDir, newRestoreDir)
	} else {
		zap.L().Info("Found previous last write timestampd, going to use backup DB first", zap.Int64("timestamp", lastWriteTsSecs))
		DBDirPrecedence = append(DBDirPrecedence, newRestoreDir, currentDBDir, newRestoreDir)
	}

	for idx, dbDir := range DBDirPrecedence {
		dirEmpty, _ := backup.DirIsEmpty(dbDir)
		if idx != 2 && dirEmpty {
			// we don't try to restore in the 3rd try, which means we are supposed create an empty database
			err = bm.RestoreLatest(dbDir)
			if err != nil {
				zap.L().Error("Failed to restore from backup", zap.Error(err))
				continue
			}
		}

		// opened successfully
		err = writeFlag(currentDirFlag, dbDir)
		if err != nil {
			return "", fmt.Errorf("failed to write to the flag file: %s, err: %v", currentDirFlag, err)
		}
		currentDBDir = dbDir
		break
	}
	purgeOldData(args.GravelDir, currentDBDir)
	if args.BackupNode {
		purgeOldBackups(bm)
	}
	return currentDirFlag, nil
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

	s3Store, err := backup.NewS3Store(args.Region, args.BackupBucket, args.ShardName)
	if err != nil {
		zap.L().Error("failed to create the s3store for backup manager", zap.Error(err))
		return Nitrous{}, err
	}

	bm, err := backup.NewBackupManager(args.PlaneID, s3Store)
	if err != nil {
		zap.L().Error("failed to create backup manager", zap.Error(err))
		return Nitrous{}, err
	}

	dir, err := dbDir(bm, args, scope)
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create db after all the tries: %v", err)
	}

	return Nitrous{
		PlaneID:              scope.ID(),
		Identity:             args.Identity,
		KafkaConsumerFactory: consumerFactory,
		Clock:                clock.New(),
		Partitions: 		  args.Partitions,
		BinlogPartitions: 	  args.BinPartitions,
		DbDir: 				  dir,
		backupManager:        bm,
	}, nil
}
