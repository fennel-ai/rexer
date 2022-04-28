package profile

import (
	"context"
	"fmt"
	"log"
	"sync"

	"fennel/controller/mock"
	"fennel/controller/profile"
	"fennel/engine/interpreter/bootarg"
	"fennel/engine/operators"
	"fennel/lib/ftypes"
	libprofile "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/tier"
)

func init() {
	operators.Register(profileOp{})
}

type profileOp struct {
	tier   tier.Tier
	args   value.Dict
	mockID int64
	cache  *sync.Map
}

func (p profileOp) New(
	args value.Dict, bootargs map[string]interface{}, cache *sync.Map,
) (operators.Operator, error) {
	tr, err := bootarg.GetTier(bootargs)
	if err != nil {
		return nil, err
	}
	var mockID int64
	mockVal, ok := args.Get("__mock_id__")
	if !ok {
		mockID = 0
	} else {
		asInt, ok := mockVal.(value.Int)
		if !ok {
			return nil, fmt.Errorf("expected '__mock_id__' to be an int but found: '%v'", mockVal)
		}
		mockID = int64(asInt)
	}
	return profileOp{tr, args, mockID, cache}, nil
}

func (p profileOp) Apply(ctx context.Context, staticKwargs value.Dict, in operators.InputIter, out *value.List) (err error) {
	var reqs []libprofile.ProfileItemKey
	var rows []value.Value
	for in.HasMore() {
		heads, kwargs, err := in.Next()
		if err != nil {
			return err
		}
		rowVal := heads[0]
		req := libprofile.ProfileItemKey{
			OType: ftypes.OType(kwargs.GetUnsafe("otype").(value.String)),
			Oid:   kwargs.GetUnsafe("oid").String(),
			Key:   string(kwargs.GetUnsafe("key").(value.String)),
		}
		reqs = append(reqs, req)
		rows = append(rows, rowVal)
	}
	var vals []value.Value
	if p.mockID != 0 {
		vals = mock.GetProfiles(reqs, p.mockID)
	} else {
		vals, err = p.getProfiles(ctx, reqs)
		if err != nil {
			return err
		}
	}
	field := string(staticKwargs.GetUnsafe("field").(value.String))
	defaultValue := staticKwargs.GetUnsafe("default")
	for i, v := range vals {
		if v == value.Nil {
			v = defaultValue
		}
		var outRow value.Value
		if len(field) > 0 {
			if d, ok := rows[i].(value.Dict); !ok {
				return fmt.Errorf("input values expected to be dict for profile operator")
			} else {
				d.Set(field, v)
				outRow = d
			}
		} else {
			outRow = v
		}
		out.Append(outRow)
	}
	return nil
}

func (p profileOp) getProfiles(ctx context.Context, profileKeys []libprofile.ProfileItemKey) ([]value.Value, error) {
	res := make([]value.Value, len(profileKeys))
	var uncached []libprofile.ProfileItemKey
	var ptrs []int
	// GetBatched returns nil for profiles that were not found
	// store in cache as it is, to avoid querying DB for profiles that we know do not exist
	// and for profile operator to set default correctly
	for i, pi := range profileKeys {
		key := p.getKey(pi)
		v, ok := p.cache.Load(key)
		if !ok {
			// did not find profile, filter out
			uncached = append(uncached, pi)
			ptrs = append(ptrs, i)
		} else {
			// found profile
			if v == nil {
				// if nil, store as it is
				res[i] = value.Nil
			} else {
				val, ok := v.(value.Value)
				if !ok {
					// this should never happen, but if it happens
					// we pretend it wasn't in cache and log error
					log.Printf("unexpected error in profile op: expected v to be a value but found '%v' instead", v)
					uncached = append(uncached, pi)
					ptrs = append(ptrs, i)
				} else {
					res[i] = val
				}
			}
		}
	}
	// now get uncached profiles
	vals, err := profile.GetBatch(ctx, p.tier, uncached)
	if err != nil {
		return nil, err
	}
	// add them to cache
	for i, pi := range uncached {
		key := p.getKey(pi)
		p.cache.Store(key, vals[i].Value)
	}

	// finally, return result
	for i, v := range vals {
		res[ptrs[i]] = v.Value
	}
	return res, nil
}

func (p profileOp) getKey(pi libprofile.ProfileItemKey) string {
	return fmt.Sprintf("profile:%s:%s:%s", pi.OType, pi.Oid, pi.Key)
}

func (p profileOp) Signature() *operators.Signature {
	return operators.NewSignature("std", "profile").
		Input([]value.Type{value.Types.Any}).
		Param("otype", value.Types.String, false, false, value.Nil).
		Param("oid", value.Types.ID, false, false, value.Nil).
		Param("key", value.Types.String, false, false, value.Nil).
		Param("version", value.Types.Int, false, true, value.Int(0)).
		Param("field", value.Types.String, true, true, value.String("")).
		Param("default", value.Types.Any, true, true, value.Nil)
}

var _ operators.Operator = profileOp{}
