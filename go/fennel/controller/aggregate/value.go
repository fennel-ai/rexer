package aggregate

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	"github.com/buger/jsonparser"

	"fennel/controller/counter"
	"fennel/engine"
	"fennel/engine/ast"
	"fennel/engine/interpreter/bootarg"
	"fennel/lib/action"
	"fennel/lib/aggregate"
	"fennel/lib/automl/vae"
	libcounter "fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/phaser"
	"fennel/lib/profile"
	"fennel/lib/value"
	"fennel/tier"

	"go.uber.org/zap"
)

const cacheValueDuration = 2 * time.Minute

// increment this to invalidate all existing cache keys for aggregate
var cacheVersion = 0

func InvalidateCache() {
	cacheVersion++
}

func Value(
	ctx context.Context, tier tier.Tier, name ftypes.AggName, key value.Value, kwargs value.Dict,
) (value.Value, error) {
	if disableCache, present := os.LookupEnv("DISABLE_CACHE"); present && disableCache == "1" {
		return unitValue(ctx, tier, name, key, kwargs)
	}

	ckey := makeCacheKey(name, key, kwargs)
	// If already present in cache and no failure interpreting it, return directly
	if v, ok := tier.PCache.Get(ckey, "AggValue"); ok {
		if val, ok2 := fromCacheValue(tier, v); ok2 {
			return val, nil
		}
	}
	// otherwise compute value and store in cache
	val, err := unitValue(ctx, tier, name, key, kwargs)
	if err != nil {
		return nil, err
	}

	if !tier.PCache.SetWithTTL(ckey, val, int64(len(ckey)+len(val.String())), cacheValueDuration, "AggValue") {
		tier.Logger.Debug(fmt.Sprintf("failed to set aggregate value in cache: key: '%s' value: '%s'", ckey, val.String()))
	}
	return val, nil
}

func BatchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	if disableCache, present := os.LookupEnv("DISABLE_CACHE"); present && disableCache == "1" {
		return batchValue(ctx, tier, batch)
	}

	ret := make([]value.Value, len(batch))
	uckeys := make([]string, len(batch))
	uncachedReqs := make([]aggregate.GetAggValueRequest, len(batch))
	ptr := make([]int, len(batch))
	seen := make(map[string]int, len(batch))

	j := 0
	for i, req := range batch {
		ckey := makeCacheKey(req.AggName, req.Key, req.Kwargs)
		if v, ok := tier.PCache.Get(ckey, "AggValue"); ok {
			if val, found := fromCacheValue(tier, v); found {
				ret[i] = val
				ptr[i] = -1
			}
		}
		// check if we could get it from cache or not
		if ptr[i] != -1 {
			// not in cache, so we may do a ground truth pull
			// but first check if this is a duplicate request
			if idx, repeat := seen[ckey]; repeat {
				ptr[i] = idx // duplicate, so use the same index
			} else {
				// if not duplicate, add to uncached requests
				seen[ckey] = j
				uckeys[j] = ckey
				uncachedReqs[j] = req
				ptr[i] = j
				j += 1
			}
		}
	}
	ucvals, err := batchValue(ctx, tier, uncachedReqs[:j])
	if err != nil {
		return nil, err
	}
	for i := range batch {
		idx := ptr[i]
		if idx < 0 {
			continue
		}
		ret[i] = ucvals[idx]
		if ok := tier.PCache.SetWithTTL(uckeys[idx], ucvals[idx], int64(len(uckeys[idx])+len(ucvals[idx].String())), cacheValueDuration, "AggValue"); !ok {
			tier.Logger.Debug(fmt.Sprintf("failed to set aggregate value in cache: key: '%s' value: '%s'", uckeys[idx], ucvals[idx].String()))
		}
	}
	return ret, nil
}

func unitValue(
	ctx context.Context, tier tier.Tier, name ftypes.AggName, key value.Value, kwargs value.Dict,
) (value.Value, error) {
	agg, err := Retrieve(ctx, tier, name)
	if err != nil {
		return value.Nil, err
	}
	return counter.Value(ctx, tier, agg.Id, key, kwargs)
}

func getDuration(kwargs value.Dict) (int, error) {
	d, ok := kwargs.Get("duration")
	if !ok {
		return 0, fmt.Errorf("error: no duration specified")
	}
	duration, ok := d.(value.Int)
	if !ok {
		return 0, fmt.Errorf("error: expected kwarg 'duration' to be an int but found: '%v'", d)
	}
	return int(duration), nil
}

func batchValue(ctx context.Context, tier tier.Tier, batch []aggregate.GetAggValueRequest) ([]value.Value, error) {
	n := len(batch)

	unique := make(map[ftypes.AggName]aggregate.Aggregate)
	for _, req := range batch {
		unique[req.AggName] = aggregate.Aggregate{}
	}
	var err error
	for name := range unique {
		unique[name], err = Retrieve(ctx, tier, name)
		if err != nil {
			tier.Logger.Error("failed to retrieve aggregate", zap.String("aggregate: ", string(name)), zap.Error(fmt.Errorf("%w", err)))
			return nil, fmt.Errorf("failed to retrieve aggregate %s", name)
		}
	}

	ret := make([]value.Value, n)
	numSlotsLeft, err := fetchOfflineAggregates(tier, unique, batch, ret)
	if err != nil {
		return nil, err
	}

	if numSlotsLeft, err = fetchAutoMLAggregates(ctx, tier, unique, batch, ret, numSlotsLeft); err != nil {
		return nil, err
	}

	if numSlotsLeft, err = fetchForeverAggregates(ctx, tier, unique, batch, ret, numSlotsLeft); err != nil {
		return nil, err
	}

	if err = fetchOnlineAggregates(ctx, tier, unique, batch, ret, numSlotsLeft); err != nil {
		return nil, err
	}
	return ret, nil
}

func fetchOfflineAggregates(tier tier.Tier, aggMap map[ftypes.AggName]aggregate.Aggregate, batch []aggregate.GetAggValueRequest, ret []value.Value) (int, error) {
	offlinePtr := make([]int, 0, len(batch))
	namespaces := make([]string, 0, len(batch))
	identifier := make([]string, 0, len(batch))
	offlineKeys := make([]value.Value, 0, len(batch))
	numSlotsLeft := len(batch)

	// Fetch offline aggregate values
	for i, req := range batch {
		agg := aggMap[req.AggName]
		if !agg.IsOffline() {
			continue
		}
		offlinePtr = append(offlinePtr, i)
		duration, err := getDuration(req.Kwargs)
		if err != nil {
			return numSlotsLeft, err
		}
		aggPhaserIdentifier := fmt.Sprintf("%s-%d", agg.Name, duration)
		namespaces = append(namespaces, OFFLINE_AGG_NAMESPACE)
		identifier = append(identifier, aggPhaserIdentifier)
		offlineKeys = append(offlineKeys, req.Key)
	}

	if len(offlinePtr) > 0 {
		offlineValues, err := phaser.BatchGet(tier, namespaces, identifier, offlineKeys)
		if err != nil {
			if err == phaser.PhaserNotFound {
				return numSlotsLeft, fmt.Errorf("error: please check duration of offline aggregate, %s", identifier)
			}
			return numSlotsLeft, err
		}

		for i, v := range offlineValues {
			ret[offlinePtr[i]] = v
			numSlotsLeft -= 1
		}
	}

	return numSlotsLeft, nil
}

func fetchAutoMLAggregates(ctx context.Context, tier tier.Tier, aggMap map[ftypes.AggName]aggregate.Aggregate, batch []aggregate.GetAggValueRequest, ret []value.Value, numSlotsLeft int) (int, error) {
	autoMLPtr := make([]int, 0, len(batch))
	ids := make([]ftypes.AggId, 0, numSlotsLeft)
	aggNames := make([]ftypes.AggName, 0, numSlotsLeft)
	keys := make([]value.Value, 0, numSlotsLeft)
	kwargs := make([]value.Dict, 0, numSlotsLeft)

	for i, req := range batch {
		agg := aggMap[req.AggName]
		if !agg.IsAutoML() {
			continue
		}
		derivedUserHistoryAggregate, err := Retrieve(ctx, tier, vae.GetDerivedUserHistoryAggregateName(agg.Name))
		if err != nil {
			return numSlotsLeft, fmt.Errorf("failed to retrieve derived user history aggregate: %w", err)
		}
		autoMLPtr = append(autoMLPtr, i)
		aggNames = append(aggNames, agg.Name)
		ids = append(ids, derivedUserHistoryAggregate.Id)
		keys = append(keys, req.Key)
		kwargs = append(kwargs, req.Kwargs)
	}

	if len(autoMLPtr) > 0 {
		userHistories, err := counter.BatchValue(ctx, tier, ids, keys, kwargs)
		if err != nil {
			return 0, fmt.Errorf("error: failed to retrieve auto-ml user history: %w", err)
		}
		// Call SageMaker endpoint to get auto-ml predictions
		aggToUserHistoryMap := make(map[ftypes.AggName][]value.Value)
		aggToPtrMap := make(map[ftypes.AggName][]int)
		for i, aggName := range aggNames {
			aggToUserHistoryMap[aggName] = append(aggToUserHistoryMap[aggName], userHistories[i])
			aggToPtrMap[aggName] = append(aggToPtrMap[aggName], autoMLPtr[i])
		}

		// TODO: parallelize this IO bound for loop.
		for aggName, userHistory := range aggToUserHistoryMap {
			smResult, err := vae.GetAutoMLPrediction(ctx, tier, aggMap[aggName], userHistory)
			if err != nil {
				return 0, fmt.Errorf("error: failed to retrieve auto-ml prediction: %w", err)
			}
			for i, ptr := range aggToPtrMap[aggName] {
				ret[ptr] = smResult[i]
				numSlotsLeft -= 1
			}
		}
	}

	return numSlotsLeft, nil
}

func fetchForeverAggregates(ctx context.Context, tier tier.Tier, aggMap map[ftypes.AggName]aggregate.Aggregate, batch []aggregate.GetAggValueRequest, ret []value.Value, numSlotsLeft int) (int, error) {
	foreverPtr := make([]int, 0, numSlotsLeft)
	foreverKeys := make([]value.Value, 0, numSlotsLeft)
	var foreverAgg aggregate.Aggregate
	var foreverKwarags value.Dict

	// Fetch forever aggregates, ( these dont need histograms )
	for i, req := range batch {
		agg := aggMap[req.AggName]
		if !agg.IsForever() {
			continue
		}

		// Current code only supports knn, need to extend this to support other aggregates
		if agg.Options.AggType != "knn" {
			return numSlotsLeft, fmt.Errorf("error: Only KNN supports forever aggregates")
		}
		foreverPtr = append(foreverPtr, i)
		foreverKeys = append(foreverKeys, req.Key)
		// Currently we assume the aggregate and kwarg is the same for all knn requests.
		foreverAgg = agg
		foreverKwarags = req.Kwargs
	}

	if len(foreverPtr) > 0 {
		if tier.MilvusClient.IsAbsent() {
			return numSlotsLeft, fmt.Errorf("error: Milvus client is not initialized")
		}
		nn, err := tier.MilvusClient.MustGet().GetNeighbors(ctx, foreverAgg, foreverKeys, foreverKwarags, tier.ID)
		if err != nil {
			return numSlotsLeft, err
		}
		for j, v := range nn {
			ret[foreverPtr[j]] = v
			numSlotsLeft -= 1
		}
	}

	return numSlotsLeft, nil
}

func fetchOnlineAggregates(ctx context.Context, tier tier.Tier, aggMap map[ftypes.AggName]aggregate.Aggregate, batch []aggregate.GetAggValueRequest, ret []value.Value, numSlotsLeft int) error {
	ids := make([]ftypes.AggId, 0, numSlotsLeft)
	keys := make([]value.Value, 0, numSlotsLeft)
	kwargs := make([]value.Dict, 0, numSlotsLeft)

	var onlinePtr []int
	// Fetch online aggregate values
	for i, req := range batch {
		agg := aggMap[req.AggName]
		if !agg.IsOnline() {
			continue
		}
		onlinePtr = append(onlinePtr, i)
		ids = append(ids, agg.Id)
		keys = append(keys, req.Key)
		kwargs = append(kwargs, req.Kwargs)
	}

	if len(onlinePtr) > 0 {
		onlineValues, err := counter.BatchValue(ctx, tier, ids, keys, kwargs)
		if err != nil {
			return err
		}

		for i, v := range onlineValues {
			ret[onlinePtr[i]] = v
		}
	}
	return nil
}

// Update the aggregates given a kafka consumer responsible for reading any stream
func Update[I action.Action | profile.ProfileItem](ctx context.Context, tier tier.Tier, items []I, agg aggregate.Aggregate) error {
	var table value.List
	var err error
	if agg.Mode == aggregate.RQL {
		table, err = Transform(tier, items, agg.Query)
	} else if agg.Mode == aggregate.PANDAS {
		table, err = TransformPandas(tier, items, agg.PythonQuery)
	}
	if err != nil {
		return fmt.Errorf("failed to Transform actions: %w", err)
	}
	if table.Len() == 0 {
		tier.Logger.Debug(fmt.Sprintf("no items to update for aggregate %s", string(agg.Name)))
		return nil
	}
	tier.Logger.Info("Processed aggregate",
		zap.String("name", string(agg.Name)),
		zap.Int("input", len(items)),
		zap.Int("output", table.Len()))
	// Update the aggregate according to the type
	if agg.IsOffline() { // Offline Aggregates
		tier.Logger.Info(fmt.Sprintf("found %d new items, %d transformed %s for offline aggregate: %s", len(items), table.Len(), agg.Source, agg.Name))
		offlineTransformProducer := tier.Producers[libcounter.AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME]
		for i := 0; i < table.Len(); i++ {
			rowVal, _ := table.At(i)
			rowDict, _ := rowVal.(value.Dict)
			dict := value.NewDict(map[string]value.Value{
				"aggregate": value.String(agg.Name),
				"groupkey":  rowDict.GetUnsafe("groupkey"),
				"value":     rowDict.GetUnsafe("value"),
				"timestamp": rowDict.GetUnsafe("timestamp"),
			})
			err = offlineTransformProducer.Log(ctx, value.ToJSON(dict), nil)
			if err != nil {
				tier.Logger.Error(fmt.Sprintf("failed to log action proto: %v", err))
			}
		}
		return nil
	} else if agg.IsForever() {
		// Forever Aggregates dont use histograms
		// Current support for only KNN, add support for other aggregates
		// https://linear.app/fennel-ai/issue/REX-1053/support-forever-aggregates
		if agg.Options.AggType != "knn" {
			return fmt.Errorf("forever aggregates are not supported for aggregate %s", agg.Name)
		}
		// Update the aggregate
		if tier.MilvusClient.IsAbsent() {
			return fmt.Errorf("error: Milvus client is not initialized")
		} else {
			// Use milvus library to update the index with all actions
			err = tier.MilvusClient.MustGet().InsertStream(ctx, agg, table, tier.ID)
			if err != nil {
				return fmt.Errorf("failed to insert stream into milvus: %w", err)
			}
		}
		return nil
	} else if agg.IsAutoML() {
		reader, writer := io.Pipe()
		// Writer should be in a separate goroutine to avoid deadlock
		// Since the pipe blocks the Writer until the data is read from the Reader,
		go func() {
			w := csv.NewWriter(writer)
			var data [][]string
			for i := 0; i < table.Len(); i++ {
				rowVal, _ := table.At(i)
				rowDict, _ := rowVal.(value.Dict)
				row := []string{string(value.ToJSON(rowDict.GetUnsafe("groupkey"))), string(value.ToJSON(rowDict.GetUnsafe("value"))), string(value.ToJSON(rowDict.GetUnsafe("timestamp")))}
				data = append(data, row)
			}
			if err = w.WriteAll(data); err != nil {
				tier.Logger.Error(fmt.Sprintf("failed to write data to file: %v", err))
				return
			}
			if err = w.Error(); err != nil {
				tier.Logger.Error(fmt.Sprintf("failed to flush data to file: %v", err))
				return
			}
			if err = writer.Close(); err != nil {
				tier.Logger.Error(fmt.Sprintf("failed to close writer: %v", err))
				return
			}
		}()
		now := tier.Clock.Now()
		year := now.Year()
		month := now.Month()
		day := now.Day()

		tier.Logger.Info(fmt.Sprintf("transformed %d events for AutoML: %s", table.Len(), agg.Name))
		path := fmt.Sprintf("automl/%s/year=%d/month=%02d/day=%02d/interactions-%d.csv", agg.Name, year, month, day, now.Unix())
		if err = tier.S3Client.Upload(reader, path, tier.Args.OfflineAggBucket); err != nil {
			return fmt.Errorf("failed to upload transformed actions for automl to s3: %w", err)
		}
		if err = reader.Close(); err != nil {
			return fmt.Errorf("failed to close reader: %w", err)
		}
	} else { // Online duration based aggregates
		tier.Logger.Info(fmt.Sprintf("found %d new items, %d transformed %s for online aggregate: %s", len(items), table.Len(), agg.Source, agg.Name))
		if err = counter.Update(ctx, tier, agg.Id, table); err != nil {
			return fmt.Errorf("failed to update counter: %w", err)
		}
		return err
	}
	return nil
}

func TransformPandas(tier tier.Tier, items any, query []byte) (value.List, error) {
	// Transform the items
	jsonPayload, err := json.Marshal(items)
	if err != nil {
		return value.NewList(), fmt.Errorf("failed to marshal items to json: %w", err)
	}
	cmd := exec.Command("python3", "controller/aggregate/transform_pandas.py", string(jsonPayload), string(query))
	out, err := cmd.Output()
	if err != nil {
		return value.NewList(), fmt.Errorf("failed to execute python script: %w", err)
	}
	transformedValues, err := value.ParseJSON(out, jsonparser.Array)
	if err != nil {
		return value.NewList(), fmt.Errorf("error in converting json to values: %v", err)
	}
	// Convert output JSON to valueList
	ret, ok := transformedValues.(value.List)
	if !ok {
		return value.NewList(), fmt.Errorf("failed to convert transformed values to value.List")
	}
	return ret, nil
}

func Transform(tier tier.Tier, items any, query ast.Ast) (value.List, error) {
	bootargs := bootarg.Create(tier)
	executor := engine.NewQueryExecutor(bootargs)
	var table value.List
	var err error
	var key string
	switch t := items.(type) {
	case []action.Action:
		key = "actions"
		table, err = action.ToList(t)
	case []profile.ProfileItem:
		key = "profiles"
		table, err = profile.ToList(t)
	case []value.Value:
		key = "stream"
		table = value.NewList(t...)
	default:
		return table, fmt.Errorf("unsupported type: %T", t)
	}
	if err != nil {
		return value.NewList(), err
	}
	if table.Len() == 0 {
		return table, nil
	}
	result, err := executor.Exec(context.Background(), query, value.NewDict(map[string]value.Value{key: table}))
	if err != nil {
		return value.NewList(), err
	}
	var ok bool
	table, ok = result.(value.List)

	if !ok {
		return value.NewList(), fmt.Errorf("query did not transform items into a list")
	}
	return table, nil
}

// ============================
// Private helpers below
// ============================

// TODO: Use AggId here as well to keep the formatting consistent with remote storage (MemoryDB)
func makeCacheKey(name ftypes.AggName, key value.Value, kwargs value.Dict) string {
	return fmt.Sprintf("%d:%s:%s:%s", cacheVersion, name, key.String(), kwargs.String())
}

func fromCacheValue(tier tier.Tier, v interface{}) (value.Value, bool) {
	switch v := v.(type) {
	case value.Value:
		return v, true
	default:
		// log unexpected error
		err := fmt.Errorf("value not of type value.Value: %v", v)
		tier.Logger.Error("aggregate value cache error: ", zap.Error(err))
		return nil, false
	}
}
