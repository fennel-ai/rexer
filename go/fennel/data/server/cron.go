package main

import (
	"fennel/client"
	"fennel/data/lib"
	"fennel/lib/action"
	lib2 "fennel/profile/lib"
	"fmt"
)

func getCheckpoint(mc MainController, ct lib.CounterType) (lib2.OidType, error) {
	return mc.checkpointTable.counterDBGetCheckpoint(ct)
}

func setCheckpoint(mc MainController, ct lib.CounterType, actionID lib2.OidType) error {
	return mc.checkpointTable.counterDBSetCheckpoint(ct, actionID)
}

// takes all recent actions since last checkpoint, computes all keys that need
// to be incremented, increments them, and sets the checkpoint as needed
func (mc MainController) run(ct lib.CounterType) error {
	client := client.NewClient("http://localhost")
	checkpoint, err := getCheckpoint(mc, ct)
	if err != nil {
		return err
	}
	actions, err := client.FetchActions(action.ActionFetchRequest{MinActionID: checkpoint})
	if err != nil {
		return err
	}
	config, ok := counterConfigs[ct]
	if !ok {
		return fmt.Errorf("config not found for counter type: %v", ct)
	}

	// TODO: batch these across actions - that way we will be able to
	// "merge" increments to many buckets (e.g. actions will likely hit
	// the same bucket for week window) and significantly reduce IO
	for _, action := range actions {
		counters := config.Generate(action, ct)
		if len(counters) == 0 {
			continue
		}
		err = Increment(mc, counters, action.Timestamp)
		if err != nil {
			return err
		}
		setCheckpoint(mc, ct, action.ActionID)
	}
	return nil
}

func Increment(mc MainController, counters []Counter, ts action.Timestamp) error {
	// TODO: make this atomic - either all the keys should persist or none should
	// otherwise, part of it can fail mid way creating inconsistency
	// either that, or make the queue we run through itself a queue of individual counters
	// instead of queue of actions
	for _, c := range counters {
		err := mc.counterTable.counterIncrement(c.Type, c.window, c.key, ts, 1)
		if err != nil {
			return err
		}
	}
	return nil
}
