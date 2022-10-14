package stream

import (
	"errors"
	"fmt"

	"fennel/lib/featurestore/schema"
	"fennel/lib/featurestore/stream/proto"
)

var ErrStreamNotFound = errors.New("stream not found")

type Stream struct {
	Name       string
	Version    uint32
	Retention  uint32
	Start      uint32
	Sources    []Source
	Connectors []Connector
	Schema     schema.Schema
}

func (s Stream) Validate() error {
	if len(s.Name) == 0 {
		return fmt.Errorf("stream name is required")
	}
	if len(s.Sources) != len(s.Connectors) {
		return fmt.Errorf("number of sources and number of connectors should be equal")
	}
	if len(s.Name) > 255 {
		return fmt.Errorf("stream name cannot be longer than 255 characters")
	}
	for i := range s.Sources {
		if s.Sources[i].GetSourceName() != s.Connectors[i].SourceName {
			return fmt.Errorf("source %d does not match source name provided in connector %d", i, i)
		}
		if err := s.Sources[i].Validate(); err != nil {
			return err
		}
		if err := s.Connectors[i].Validate(); err != nil {
			return err
		}
	}
	return nil
}

func FromRequest(req *proto.CreateStreamRequest) (strm Stream, err error) {
	strm.Name = req.Name
	strm.Version = req.Version
	strm.Retention = req.Retention
	strm.Start = req.Start
	strm.Sources = make([]Source, len(req.Sources))
	for i := range req.Sources {
		strm.Sources[i], err = SourceFromRequest(req.Sources[i])
		if err != nil {
			return Stream{}, err
		}
	}
	strm.Connectors = make([]Connector, len(req.Connectors))
	for i := range req.Connectors {
		strm.Connectors[i] = ConnectorFromRequest(req.Connectors[i], req.Name)
	}
	strm.Schema = schema.FromProto(req.Schema)

	return strm, nil
}

func (s Stream) Equals(other Stream) error {
	// TODO: maybe
	// does not check if sources and connectors are same
	// does not check if schema is the same
	if s.Name != other.Name {
		return fmt.Errorf("stream name mismatch")
	}
	if s.Version != other.Version {
		return fmt.Errorf("version mismatch")
	}
	if s.Retention != other.Retention {
		return fmt.Errorf("retention mismatch")
	}
	if s.Start != other.Start {
		return fmt.Errorf("start mismatch")
	}
	return nil
}
