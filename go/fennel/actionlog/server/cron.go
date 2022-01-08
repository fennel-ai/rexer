package main

import (
	"fennel/actionlog/client"
	"fennel/actionlog/lib"
	"fmt"
)

func getCheckpoint(ct lib.CounterType) (lib.OidType, error) {
	return counterDBGetCheckpoint(ct)
}

func setCheckpoint(ct lib.CounterType, actionID lib.OidType) error {
	return counterDBSetCheckpoint(ct, actionID)
}

// takes all recent actions since last checkpoint, computes all keys that need
// to be incremented, increments them, and sets the checkpoint as needed
func run(ct lib.CounterType) error {
	client := client.NewClient("http://localhost")
	checkpoint, err := getCheckpoint(ct)
	if err != nil {
		return err
	}
	actions, err := client.Fetch(lib.ActionFetchRequest{MinActionID: checkpoint})
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
		err = Increment(counters, action.Timestamp)
		if err != nil {
			return err
		}
		setCheckpoint(ct, action.ActionID)
	}
	return nil
}
