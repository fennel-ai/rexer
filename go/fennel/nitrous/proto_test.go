package nitrous

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtoGetManyRequest(t *testing.T) {
	scenarios := []struct {
		reqs []GetReq
		err  bool
	}{
		{reqs: []GetReq{}, err: false},
		{reqs: []GetReq{
			{1, "a", []string{"a", "b", "c"}},
			{2, "somekey", []string{"a", "23", ""}},
		}, err: true},
		{reqs: []GetReq{
			{2, "a", []string{"a", "b", "c"}},
			{2, "somekey", []string{"a", "23", ""}},
			{2, "c", []string{}},
		}, err: false},
	}
	for i, scenario := range scenarios {
		preq, err := ToProtoGetManyRequest(scenario.reqs)
		if scenario.err {
			assert.Errorf(t, err, "scenario %d", i)
		} else {
			assert.NoError(t, err)
			found, err := FromProtoGetManyRequest(preq)
			assert.NoError(t, err)
			assert.Equal(t, scenario.reqs, found)
		}
	}
}

func TestProtoGetManyResponse(t *testing.T) {
	scenarios := []struct {
		resps []GetResp
		err   bool
	}{
		{resps: []GetResp{}, err: false},
		{resps: []GetResp{
			{1, "a", map[string]string{"a": "b", "c": "d"}},
			{2, "somekey", map[string]string{"a": "23", "": "someval"}},
		}, err: true},
		{resps: []GetResp{
			{2, "a", map[string]string{"a": "b", "c": "d"}},
			{2, "somekey", map[string]string{"a": "23", "": "someval"}},
			{2, "c", map[string]string{}},
		}, err: false},
	}
	for i, scenario := range scenarios {
		preq, err := ToProtoGetManyResponse(scenario.resps)
		if scenario.err {
			assert.Errorf(t, err, "scenario %d", i)
		} else {
			assert.NoError(t, err)
			found, err := FromProtoGetManyResponse(preq)
			assert.NoError(t, err)
			assert.Equal(t, scenario.resps, found)
		}
	}
}

func TestProtoSetRequest(t *testing.T) {
	scenarios := []struct {
		req SetReq
		err bool
	}{
		{req: SetReq{TierID: 1, Base: "a", Data: map[string]string{"a": "b", "c": "d"}, Expires: 123}, err: false},
		{req: SetReq{TierID: 0, Base: "a", Data: map[string]string{"some thing longer": "b", "c": "d"}, Expires: 0}, err: false},
	}
	for _, scenario := range scenarios {
		preq, err := ToProtoSetRequest(scenario.req)
		if scenario.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			found, err := FromProtoSetRequest(preq)
			assert.NoError(t, err)
			assert.Equal(t, scenario.req, found)
		}
	}
}

func TestProtoDelRequest(t *testing.T) {
	scenarios := []struct {
		req DelReq
		err bool
	}{
		{req: DelReq{TierID: 1, Base: "a", Indices: []string{"a", "b", "c"}}, err: false},
		{req: DelReq{TierID: 0, Base: "a", Indices: []string{"some thing longer", "b", "c"}}, err: false},
	}
	for _, scenario := range scenarios {
		preq, err := ToProtoDelRequest(scenario.req)
		if scenario.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			found, err := FromProtoDelRequest(preq)
			assert.NoError(t, err)
			assert.Equal(t, scenario.req, found)
		}
	}
}
