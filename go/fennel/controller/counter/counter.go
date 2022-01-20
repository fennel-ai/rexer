package counter

import (
	"fennel/controller/action"
	"fennel/instance"
	actionlib "fennel/lib/action"
	counterlib "fennel/lib/counter"
	"fennel/model/checkpoint"
	"fennel/model/counter"
	"fmt"
)

// Aggregate for the given counter type, takes all recent actions since last checkpoint,
// computes all keys that need to be incremented, increments them, and sets the
// checkpoint as needed
func Aggregate(this instance.Instance, ct counterlib.CounterType) error {
	lastCheckPoint, err := checkpoint.GetCheckpoint(this, ct)
	if err != nil {
		return err
	}
	actions, err := action.Fetch(this, actionlib.ActionFetchRequest{MinActionID: lastCheckPoint})
	if err != nil {
		return err
	}
	config, ok := counterConfigs[ct]
	if !ok {
		return fmt.Errorf("config not found for counterlib type: %v", ct)
	}

	// TODO: batch these across actions - that way we will be able to
	// "merge" increments to many buckets (e.g. actions will likely hit
	// the same bucket for week window) and significantly reduce IO
	for _, action := range actions {
		counters := config.Generate(action, ct)
		if len(counters) == 0 {
			continue
		}
		err = increment(this, counters, action.Timestamp)
		if err != nil {
			return err
		}
		checkpoint.SetCheckpoint(this, ct, action.ActionID)
	}
	return nil
}

func increment(this instance.Instance, counters []Counter, ts actionlib.Timestamp) error {
	// TODO: make this atomic - either all the keys should persist or none should
	// otherwise, part of it can fail mid way creating inconsistency
	// either that, or make the queue we Aggregate through itself a queue of individual counters
	// instead of queue of actions
	for _, c := range counters {
		err := counter.Increment(this, c.Type, c.window, c.key, ts, 1)
		if err != nil {
			return err
		}
	}
	return nil
}

func Types() []counterlib.CounterType {
	ret := make([]counterlib.CounterType, 0)
	for ct, _ := range counterConfigs {
		ret = append(ret, ct)
	}
	return ret
}

func Count(this instance.Instance, request counterlib.GetCountRequest) (uint64, error) {
	return counter.Get(this, request)
}

func Rate(this instance.Instance, request counterlib.GetRateRequest) (float64, error) {
	numRequest := counterlib.GetCountRequest{CounterType: request.NumCounterType, Window: request.Window, Key: request.NumKey, Timestamp: request.Timestamp}
	numCount, err := Count(this, numRequest)
	if err != nil {
		return 0, err
	}
	denRequest := counterlib.GetCountRequest{CounterType: request.DenCounterType, Window: request.Window, Key: request.DenKey, Timestamp: request.Timestamp}
	denCount, err := Count(this, denRequest)
	return wilson(numCount, denCount, request.LowerBound)
}
