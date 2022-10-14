package aggregate

import (
	"fennel/lib/featurestore/schema"
)

type Aggregate struct {
	Name               string
	Mode               string
	Version            uint32
	Stream             string
	AggCls             []byte
	FunctionSourceCode string
	Type               Type
	Windows            []int32
	Schema             schema.Schema
	Dependencies       []string
}

type Type struct {
	Function       int
	KeyFields      []string
	TimestampField string
	Config         Config
}

type Config interface {
	IsConfig()
}

var _ Config = WindowConfig{}
var _ Config = KeyValueConfig{}
var _ Config = TopKConfig{}
var _ Config = CFConfig{}

type WindowConfig struct {
	Windows    []int32
	ValueField string
}

func (c WindowConfig) IsConfig() {}

type KeyValueConfig struct {
	ValueField string
}

func (c KeyValueConfig) IsConfig() {}

type TopKConfig struct {
	K          int32
	ItemFields []string
	ScoreField string
}

func (c TopKConfig) IsConfig() {}

type CFConfig struct {
	K             int32
	ContextFields []string
	WeightField   string
}

func (c CFConfig) IsConfig() {}
