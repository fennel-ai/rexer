package main

import (
	"fennel/client"
	"fennel/lib/action"
	"fennel/lib/counter"
	lib2 "fennel/lib/profile"
	"fennel/value"
	"fmt"
)

func actorID(actorID lib2.OidType, actorType lib2.OType, targetID lib2.OidType, targetType lib2.OType) []counter.Key {
	return []counter.Key{
		{actorID},
	}
}

func targetID(actorID lib2.OidType, actorType lib2.OType, targetID lib2.OidType, targetType lib2.OType) []counter.Key {
	return []counter.Key{
		{targetID},
	}
}
func actorTargetID(actorID lib2.OidType, actorType lib2.OType, targetID lib2.OidType, targetType lib2.OType) []counter.Key {
	return []counter.Key{
		{actorID, targetID},
	}
}

func prefixWithIDList(prefix counter.Key, idList value.Value) []counter.Key {
	if ids, ok := idList.(value.List); ok {
		ret := make([]counter.Key, 0)
		for _, id := range ids {
			if idInt, ok := id.(value.Int); ok {
				next := make(counter.Key, len(prefix)+1)
				copy(next, prefix)
				next = append(next, lib2.OidType(idInt))
				ret = append(ret, next)
			}
		}
		return ret
	}
	return []counter.Key{}
}

func profile(otype uint32, oid uint64, key string, version uint64) (*value.Value, error) {
	// TODO: how does this code discover the port/url for profile service?
	c := client.NewClient("")
	req := lib2.NewProfileItem(otype, oid, key, version)
	return c.GetProfile(&req)
}

type Keygen func(actorID lib2.OidType, actorType lib2.OType, targetID lib2.OidType, targetType lib2.OType) []counter.Key

func init() {
	counterConfigs = map[counter.CounterType]CounterConfig{
		counter.CounterType_USER_LIKE:       {actorType: lib2.User, actionType: action.Like, keygen: actorID},
		counter.CounterType_USER_VIDEO_LIKE: {actorType: lib2.User, actionType: action.Like, targetType: lib2.Video, keygen: actorTargetID},
		counter.CounterType_VIDEO_LIKE:      {targetType: lib2.Video, actionType: action.Like, keygen: targetID},

		// These are commented for unit tests to work
		// Eventually remove these from here and just add more tests with these
		//CounterType_USER_SHARE:      {actorType: lib.User, actionType: lib.Share, keygen: actorID},
		//VIDEO_SHARE:     {targetType: lib.Video, actionType: lib.Share, keygen: targetID},
		//USER_ACCOUNT_LIKE: {actorType: lib.User, targetType: lib.Video, actionType: lib.Like,
		//	keygen: func(actorID lib.OidType, actorType lib.OType, targetID lib.OidType, targetType lib.OType) []lib.Key {
		//		account, err := profile(targetType, targetID, "account", 0)
		//		if err == nil && account != nil {
		//			if accountID, ok := (*account).(value.Int); ok {
		//				return []lib.Key{{actorID, lib.OidType(accountID)}}
		//			}
		//		}
		//		return []lib.Key{}
		//	},
		//},
		//USER_TOPIC_LIKE: {actorType: lib.User, actionType: lib.Like, targetType: lib.Video,
		//	keygen: func(actorID lib.OidType, actorType lib.OType, targetID lib.OidType, targetType lib.OType) []lib.Key {
		//		topicids, err := profile(targetType, targetID, "topic", 0)
		//		if err == nil && topicids != nil {
		//			return prefixWithIDList(lib.Key{actorID}, *topicids)
		//		}
		//		return []lib.Key{}
		//	},
		//},
		//AGE_VIDEO_LIKE: {actorType: lib.User, actionType: lib.Like, targetType: lib.Video,
		//	keygen: func(actorID lib.OidType, actorType lib.OType, _ lib.OidType, _ lib.OType) []lib.Key {
		//		age, err := profile(actorType, actorID, "age", 0)
		//		if err == nil {
		//			return []lib.Key{{actorID, lib.OidType((*age).(value.Int) / 5)}}
		//		} else {
		//			return []lib.Key{}
		//		}
		//	},
		//},
		// TODO: implement all other counter-configs
	}
}

type Counter struct {
	Type       counter.CounterType
	key        []lib2.OidType
	actionType action.ActionType
	window     counter.Window
}

// TODO: make it possible to optionally restrict CounterConfig to be only certain time windows
// NOTE: each counter config must specific exactly one event type
type CounterConfig struct {
	actorType  lib2.OType
	targetType lib2.OType
	actionType action.ActionType
	filter     func(action.Action) bool
	keygen     Keygen
}

func (cg CounterConfig) Validate() error {
	// TODO: verifyFetch that action_type type isn't too large (compared to the hardcoded list)
	// TODO: verifyFetch that actor_type and target_type if non-zero are valid
	// TODO: verify that at least one keygen is given
	if cg.actionType <= 0 {
		return fmt.Errorf("counter config not given a valid action_type type")
	}
	return nil
}

func (cg CounterConfig) Generate(a action.Action, type_ counter.CounterType) []Counter {
	if cg.actionType != a.ActionType {
		return []Counter{}
	}

	if cg.actorType > 0 && cg.actorType != a.ActorType {
		return []Counter{}
	}

	keys := cg.keygen(a.ActorID, a.ActorType, a.TargetID, a.TargetType)
	ret := make([]Counter, 0)
	for _, key := range keys {
		for _, w := range counter.Windows() {
			ret = append(ret, Counter{Type: type_, key: key, actionType: cg.actionType, window: w})
		}
	}
	return ret
}

var counterConfigs map[counter.CounterType]CounterConfig
