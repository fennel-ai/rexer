package nitrous

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"fennel/hangar"
	"fennel/hangar/cache"
	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/hangar/layered"
	libkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/nitrous/backup"
	"fennel/resource"
	"fennel/s3"

	"github.com/raulk/clock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type NitrousArgs struct {
	s3.S3Args          `json:"s3_._s3_args"`
	PlaneID            ftypes.RealmID `arg:"--plane-id,env:PLANE_ID" json:"plane_id,omitempty"`
	KafkaServer        string         `arg:"--kafka-server,env:KAFKA_SERVER_ADDRESS" json:"kafka_server,omitempty"`
	KafkaUsername      string         `arg:"--kafka-user,env:KAFKA_USERNAME" json:"kafka_username,omitempty"`
	KafkaPassword      string         `arg:"--kafka-password,env:KAFKA_PASSWORD" json:"kafka_password,omitempty"`
	BadgerDir          string         `arg:"--badger_dir,env:BADGER_DIR" json:"badger_dir,omitempty"`
	BadgerBlockCacheMB int64          `arg:"--badger_block_cache_mb,env:BADGER_BLOCK_CACHE_MB" json:"badger_block_cache_mb,omitempty"`
	RistrettoMaxCost   uint64         `arg:"--ristretto_max_cost,env:RISTRETTO_MAX_COST" json:"ristretto_max_cost,omitempty"`
	RistrettoAvgCost   uint64         `arg:"--ristretto_avg_cost,env:RISTRETTO_AVG_COST" json:"ristretto_avg_cost,omitempty" default:"100"`
	Dev                bool           `arg:"--dev" default:"true" json:"dev,omitempty"`
	BackupNode         bool           `arg:"--backup-node" json:"backup_node,omitempty"`
	BackupBucket       string         `arg:"--backup-bucket,env:BACKUP_BUCKET" json:"backup_bucket,omitempty"`
	ShardName          string         `arg:"--shard-name,env:SHARD_NAME" default:"default" json:"shard_name,omitempty"`
}

/*
=======
	// Restore aggregate definitions.
	adm := metadata.NewAggDefsMgr(plane, tailer)
	if err != nil {
		return fmt.Errorf("failed to setup aggregate definitions manager: %w", err)
	}
	err = adm.RestoreAggregates()
	if err != nil {
		return fmt.Errorf("failed to restore aggregate definitions: %w", err)
	}

	// Start tailing the binlog. We do this after restoring the aggregates, so
	// that they don't miss any events.
	go tailer.Tail()

	// Setup server.
	svr := server.NewServer(adm, tailer)

	// Setup the grpc server. Add a prometheus middleware to the main router to
	// capture standard metrics.
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	rpc.RegisterNitrousServer(grpcServer, svr)
	// After all your registrations, make sure all of the Prometheus metrics are initialized.
	grpc_prometheus.Register(grpcServer)

	// Finally, start the server.
	if err = grpcServer.Serve(listener); err != nil {
		return fmt.Errorf("failed to serve: %w", err)
	}
>>>>>>> bb492ded (nitrous_backup progress)
*/

func (args NitrousArgs) Valid() error {
	// TODO: implement
	return nil
}

type KafkaConsumerFactory func(libkafka.ConsumerConfig) (libkafka.FConsumer, error)

type Nitrous struct {
	PlaneID              ftypes.RealmID
	Logger               *zap.Logger
	Clock                clock.Clock
	Store                hangar.Hangar
	KafkaConsumerFactory KafkaConsumerFactory
	DataDir              string
}

func WriteFlag(flagFileName string, value string) error {
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

func ReadFlag(flagFileName str) string {
	content, err := ioutil.ReadFile(flagFileName)
	if err != nil {
		return ""
	}
	return string(content)
}

func Restore(bm *backup.BackupManager, dbDir string, logger *zap.Logger) error {
	backups, err := bm.ListBackups()
	if err != nil {
		return err
	}
	if len(backups) == 0 {
		logger.Warn("There is no previous backups")
		return nil
	}
	sort.Strings(backups)
	backupToRecover := backups[len(backups)-1]
	logger.Info(fmt.Sprintf("Going to restorethe lastest backup: %s", backupToRecover))
	err = bm.RestoreToPath(dbDir, backupToRecover)
	if err != nil {
		return err
	}
	logger.Info("Successfully restored the latest backup")
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
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to construct logger: %w", err)
	}
	logger = logger.With(zap.Uint32("plane_id", args.PlaneID.Value()))

	// Initialize kafka consumer factory.
	consumerFactory := func(config libkafka.ConsumerConfig) (libkafka.FConsumer, error) {
		kafkaConsumerConfig := libkafka.RemoteConsumerConfig{
			BootstrapServer: args.KafkaServer,
			Username:        args.KafkaUsername,
			Password:        args.KafkaPassword,
			ConsumerConfig:  config,
		}
		kafkaConsumer, err := kafkaConsumerConfig.Materialize()
		if err != nil {
			return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		return kafkaConsumer.(libkafka.FConsumer), nil
	}

	s3Store, err := backup.NewS3Store(args.Region, args.BackupBucket, args.ShardName, logger)
	if err != nil {
		return Nitrous{}, err
	}

	bm, err := backup.NewBackupManager(args.PlaneID, logger, s3Store)
	if err != nil {
		return Nitrous{}, err
	}

	currentDirFlag := args.BadgerDir + "/current_data_folder.txt"

	newRestoreDir := fmt.Sprintf("%s/%d", args.BadgerDir, time.Now().Unix())
	err = os.Mkdir(newRestoreDir, os.ModePerm)
	if err != nil {
		return Nitrous{}, err
	}

	// Initialize layered storage.
	currentDBDir := ReadFlag(currentDirFlag)
	lastWriteTs, _ := strconv.ParseInt(ReadFlag(currentDirFlag+"/last_write_minute.flag"), 10, 64)
	var DBDirPrecedence []string

	if lastWriteTs+7200 >= time.Now().Unix() {
		// pull from the last DB
		logger.Info(fmt.Sprintf("Found previous last write timestamp %d, going to reload the previous DB first", lastWriteTs))
		DBDirPrecedence = append(DBDirPrecedence, currentDBDir, newRestoreDir, newRestoreDir)
	} else {
		logger.Info(fmt.Sprintf("Found previous last write timestamp %d, going to use backup DB first", lastWriteTs))
		DBDirPrecedence = append(DBDirPrecedence, newRestoreDir, currentDBDir, newRestoreDir)
	}

	var dbIns hangar.Hangar
	for idx, dbDir := range DBDirPrecedence {
		dirEmpty, _ := backup.DirIsEmpty(dbDir)
		if idx != 2 && dirEmpty {
			// we don't try to restore in the 3rd try, which means we are supposed create an empty database
			err = Restore(bm, dbDir, logger)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to restore from backup %v", err))
				continue
			}
		}

		dbIns, err = db.NewHangar(scope.ID(), dbDir, args.BadgerBlockCacheMB<<20, encoders.Default(), bm)
		if err != nil {
			logger.Error(fmt.Sprintf("failed to create badger db: %v", err))
			continue
		} else {
			// opened successfully
			err = WriteFlag(currentDirFlag, dbDir)
			if err != nil {
				return Nitrous{}, fmt.Errorf("failed to write current dir flag file: %w", err)
			}
			currentDBDir = dbDir
			// clear other directories
		}
		break
	}

	if dbIns == nil {
		return Nitrous{}, fmt.Errorf("failed to create db after all the tries")
	}

	cacheIns, err := cache.NewHangar(scope.ID(), args.RistrettoMaxCost, args.RistrettoAvgCost, encoders.Default())
	if err != nil {
		return Nitrous{}, fmt.Errorf("failed to create cache: %w", err)
	}
	layeredIns := layered.NewHangar(scope.ID(), cacheIns, dbIns)

	return Nitrous{
		PlaneID:              scope.ID(),
		KafkaConsumerFactory: consumerFactory,
		Clock:                clock.New(),
		Logger:               logger,
		Store:                layeredIns,
		DataDir:              currentDBDir,
	}, nil
	/*
		=======
		func StartBackupNode(plane plane.Plane) error {
			// Initialize binlog tailer.
			offsetkey := []byte("default_tailer")
			vgs, err := plane.Store.GetMany([]hangar.KeyGroup{{Prefix: hangar.Key{Data: offsetkey}}})
			if err != nil {
				return fmt.Errorf("failed to get binlog offsets: %w", err)
			}
			var toppars kafka.TopicPartitions
			if len(vgs) > 0 {
				toppars, err = offsets.DecodeOffsets(vgs[0])
				if err != nil {
					plane.Logger.Fatal("Failed to restore binlog offsets from hangar", zap.Error(err))
				}
			}
			tailer, err := tailer.NewTailer(plane, nitrous.BINLOG_KAFKA_TOPIC, toppars, offsetkey)
			if err != nil {
				return fmt.Errorf("failed to setup tailer: %w", err)
			}

			// Restore aggregate definitions.
			adm := metadata.NewAggDefsMgr(plane, tailer)
			if err != nil {
				return fmt.Errorf("failed to setup aggregate definitions manager: %w", err)
			}
			err = adm.RestoreAggregates()
			if err != nil {
				return fmt.Errorf("failed to restore aggregate definitions: %w", err)
			}

			for {
				// Start tailing the binlog. We do this after restoring the aggregates, so
				// that they don't miss any events.
				tailer.Tail()
				time.Sleep(time.Hour)
				tailer.Stop()
				_, err = plane.Store.Backup(nil, 0)
				if err != nil {
					plane.Logger.Error(fmt.Sprintf("failed to create backup: %v", err))
				}
			}
		>>>>>>> bb492ded (nitrous_backup progress)
	*/
}
