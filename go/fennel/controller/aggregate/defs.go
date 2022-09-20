package aggregate

import (
	"context"
	"errors"
	"fennel/lib/aggregate"
	"fennel/lib/automl/vae"
	"fennel/lib/ftypes"
	"fennel/lib/phaser"
	modelAgg "fennel/model/aggregate"
	nitrous "fennel/nitrous/client"
	"fennel/tier"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Data is kept in redis OFFLINE_AGG_TTL_MULTIPLIER times the update frequency
var OFFLINE_AGG_TTL_MULTIPLIER = 3
var OFFLINE_AGG_NAMESPACE = "agg"
var SAGEMAKER_JOB_CREATION_ROLE_ARN = "arn:aws:iam::030813887342:role/sagemaker-pipeline-executor"

// getUpdateFrequency returns the update frequency in hours from the cron schedule
func getUpdateFrequency(cron string) (time.Duration, error) {
	// parts refers to the parts ( min, hour, day(month), month, day(week)) of the cron schedule
	parts := strings.Split(cron, " ")
	if len(parts) != 5 {
		return 0, fmt.Errorf("invalid cron schedule: %s", cron)
	}

	if strings.Contains(parts[1], "/") {
		x, _ := strconv.Atoi(strings.Replace(parts[1], "*/", "", 1))
		return time.Duration(x) * time.Hour, nil
	} else if strings.Contains(parts[2], "/") {
		x, _ := strconv.Atoi(strings.Replace(parts[2], "*/", "", 1))
		return time.Duration(x) * time.Hour * 24, nil
	}
	return 0, fmt.Errorf("cron schedule is not valid, reached end of function")
}

func Store(ctx context.Context, tier tier.Tier, agg aggregate.Aggregate) error {
	if err := agg.Validate(); err != nil {
		return err
	}
	// Check if agg already exists in db
	agg2, err := modelAgg.Retrieve(ctx, tier, agg.Name)
	if err != nil {
		if errors.Is(err, aggregate.ErrNotFound) {
			if agg.IsOffline() {
				// If offline aggregate, write to AWS Glue
				err := tier.GlueClient.ScheduleOfflineAggregate(tier.ID, agg)
				if err != nil {
					return err
				}
				for _, duration := range agg.Options.Durations {
					prefix := fmt.Sprintf("t_%d/%s-%d", int(tier.ID), agg.Name, duration)
					aggPhaserIdentifier := fmt.Sprintf("%s-%d", agg.Name, duration)
					ttl, err := getUpdateFrequency(agg.Options.CronSchedule)
					if err != nil {
						return err
					}
					ttl *= time.Duration(OFFLINE_AGG_TTL_MULTIPLIER)
					err = phaser.NewPhaser(OFFLINE_AGG_NAMESPACE, aggPhaserIdentifier, tier.Args.OfflineAggBucket, prefix, ttl, tier)
					if err != nil {
						return err
					}
				}
			} else if agg.IsAutoML() {
				// Fetch the ARN for pipeline
				pipelineARN, err := vae.GetPipelineARN(ctx, tier, string(agg.Options.AggType))
				if err != nil {
					return fmt.Errorf("failed to get pipeline ARN: %w", err)
				}

				// Create a recurring job for the pipeline
				ruleName := fmt.Sprintf("%s-%s", agg.Name, "automl")
				if err = tier.EventBridgeClient.CreateRule(tier.ID, ruleName, "cron("+agg.Options.CronSchedule+" *)"); err != nil {
					return fmt.Errorf("failed to create rule for recurring schedule for automl: %w", err)
				}

				smParams := vae.GetSageMakerPipelineParams(tier.ID, agg.Name, tier.Args.OfflineAggBucket)
				if err = tier.EventBridgeClient.CreateSageMakeRecurringJob(tier.ID, ruleName, pipelineARN, SAGEMAKER_JOB_CREATION_ROLE_ARN, smParams); err != nil {
					return fmt.Errorf("failed to recurring job for automl: %w", err)
				}

				if err = Store(ctx, tier, vae.GetUserHistoryAggregate(agg)); err != nil {
					return fmt.Errorf("failed to store user history aggregate: %w", err)
				}
			}

			tier.Logger.Debug("Storing new aggregate")
			if agg.Timestamp == 0 {
				agg.Timestamp = ftypes.Timestamp(time.Now().Unix())
			}
			agg.Active = true
			if agg.Options.AggType == "knn" {
				if tier.MilvusClient.IsAbsent() {
					return fmt.Errorf("milvus client is not configured for this tier")
				} else {
					// Call into Milvus to create the knn index
					err = tier.MilvusClient.MustGet().CreateKNNIndex(ctx, agg, tier.ID)
					if err != nil {
						return err
					}
				}
			}
			// Store aggregate in db.
			err = modelAgg.Store(ctx, tier, agg)
			if err != nil {
				return err
			}
			// Forward online aggregate definition to nitrous.
			if !agg.IsOffline() && !agg.IsAutoML() {
				// Note: we retrieve the aggregate back from the db since agg.Id
				// is not initialized yet.
				agg, err = modelAgg.Retrieve(ctx, tier, agg.Name)
				if err != nil {
					return fmt.Errorf("failed to retrieve aggregate %s after creating: %w", agg.Name, err)
				}
				go createOnNitrous(ctx, tier, agg.Id, agg.Options)
			}
			return nil
		} else {
			return fmt.Errorf("failed to retrieve aggregate: %w", err)
		}
	} else {
		// if already present, check if query and options are the same
		// if they are the same, activate the aggregate in case it was deactivated.
		// if they are different, return error
		queryEquals := false
		if agg.Mode == agg2.Mode && agg.Mode == aggregate.RQL && agg.Query == agg2.Query {
			queryEquals = true
		}
		if agg.Mode == agg2.Mode && agg.Mode == aggregate.PANDAS && string(agg.PythonQuery) == string(agg2.PythonQuery) {
			queryEquals = true
		}
		if queryEquals && agg.Options.Equals(agg2.Options) && agg.Source == agg2.Source {
			if !agg2.Active {
				err := modelAgg.Activate(ctx, tier, agg.Name)
				if err != nil {
					return fmt.Errorf("failed to reactivate aggregate '%s': %w", agg.Name, err)
				}
			}
			// Forward online aggregates to nitrous if the client has been initialized.
			// We do this even if the aggregate has been previously defined.
			if !agg.IsOffline() {
				// Note: we send agg2.Id since agg.Id is not initialized yet.
				go createOnNitrous(ctx, tier, agg2.Id, agg.Options)
			}
			return nil
		} else {
			return fmt.Errorf("already present but with different query/options")
		}
	}
}

func createOnNitrous(ctx context.Context, tier tier.Tier, aggId ftypes.AggId, aggOptions aggregate.Options) {
	var err error
	tier.NitrousClient.ForEach(func(nc nitrous.NitrousClient) {
		err = nc.CreateAggregate(ctx, aggId, aggOptions)
	})
	if err != nil {
		tier.Logger.Warn("Failed to forward aggregate definition to nitrous", zap.Error(err))
	} else {
		tier.Logger.Debug("Forwarded aggregate definition to nitrous", zap.Int("aggId", int(aggId)))
	}
}

func deleteFromNitrous(ctx context.Context, tier tier.Tier, aggId ftypes.AggId) {
	var err error
	tier.NitrousClient.ForEach(func(nc nitrous.NitrousClient) {
		err = nc.DeleteAggregate(ctx, aggId)
	})
	if err != nil {
		tier.Logger.Warn("Failed to forward aggregate deletion to nitrous", zap.Error(err))
	} else {
		tier.Logger.Debug("Forwarded aggregate deletion to nitrous")
	}
}

func Retrieve(ctx context.Context, tier tier.Tier, aggname ftypes.AggName) (aggregate.Aggregate, error) {
	empty := aggregate.Aggregate{}
	if len(aggname) == 0 {
		return empty, fmt.Errorf("aggregate name can not be of length zero")
	}
	var agg aggregate.Aggregate
	if def, ok := tier.AggregateDefs.Load(aggname); !ok {
		var err error
		agg, err = modelAgg.Retrieve(ctx, tier, aggname)
		if err != nil {
			return empty, fmt.Errorf("failed to get aggregate: %w", err)
		}
		if !agg.Active {
			return agg, aggregate.ErrNotActive
		}
		tier.AggregateDefs.Store(aggname, agg)
	} else {
		agg = def.(aggregate.Aggregate)
	}
	return agg, nil
}

// RetrieveActive returns all active aggregates
func RetrieveActive(ctx context.Context, tier tier.Tier) ([]aggregate.Aggregate, error) {
	return modelAgg.RetrieveActive(ctx, tier)
}

func RunAggregate(ctx context.Context, tier tier.Tier, aggname ftypes.AggName, duration int) error {
	agg, err := Retrieve(ctx, tier, aggname)
	if err != nil {
		return err
	}
	if !agg.IsOffline() {
		return fmt.Errorf("only offline computed aggregates can be run")
	}
	found := false
	for _, d := range agg.Options.Durations {
		if d == uint32(duration) {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("duration %d not found in aggregate %s", duration, aggname)
	}
	return tier.GlueClient.StartAggregate(tier.ID, agg, duration)
}

func Deactivate(ctx context.Context, tier tier.Tier, aggname ftypes.AggName) error {
	if len(aggname) == 0 {
		return fmt.Errorf("aggregate name can not be of length zero")
	}
	// Remove if present in cache
	tier.AggregateDefs.Delete(aggname)
	// Check if agg already exists in db
	agg, err := modelAgg.Retrieve(ctx, tier, aggname)
	// If it is absent, it returns aggregate.ErrNotFound
	// If any other error, return it as well
	if err != nil {
		return err
	}

	// Forward online aggregate deletions to nitrous.
	if !agg.IsOffline() {
		go deleteFromNitrous(ctx, tier, agg.Id)
	}

	// If it is present and inactive, do nothing
	// otherwise, deactivate
	if !agg.Active {
		return nil
	} else {
		// deactive trigger only if the aggregate is offline
		if agg.IsOffline() {
			if err := tier.GlueClient.DeactivateOfflineAggregate(tier.ID, string(aggname)); err != nil {
				return err
			}
			for _, duration := range agg.Options.Durations {
				aggPhaserIdentifier := fmt.Sprintf("%s-%d", agg.Name, duration)
				err = phaser.DeletePhaser(tier, OFFLINE_AGG_NAMESPACE, aggPhaserIdentifier)
				if err != nil {
					return err
				}
			}
		}
		if agg.Options.AggType == "knn" {
			if tier.MilvusClient.IsAbsent() {
				return fmt.Errorf("milvus client is not configured for this tier")
			} else {
				// Call into Milvus to delete the knn index
				err = tier.MilvusClient.MustGet().DeleteCollection(ctx, agg.Name, tier.ID)
				if err != nil {
					return err
				}
			}
		}

		// Disable online & offline aggregates
		err = modelAgg.Deactivate(ctx, tier, aggname)
		return err
	}
}
