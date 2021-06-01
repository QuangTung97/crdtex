package crdtex

import (
	"context"
	"github.com/google/uuid"
)

// Entry ...
type Entry struct {
	Seq     uint64
	NodeID  uuid.UUID
	Version uint64
}

// State ...
type State map[string]Entry

//go:generate moq -out interface_mock_test.go . Interface

// Interface ...
type Interface interface {
	Start(ctx context.Context, finish chan<- struct{})
	InitConn(addr string)
	UpdateRemote(ctx context.Context, addr string, state State) (State, error)
}

func entryLess(a, b Entry) bool {
	if a.Seq < b.Seq {
		return true
	}
	if a.Seq == b.Seq {
		if uuidLess(a.NodeID, b.NodeID) {
			return true
		}
		if a.NodeID == b.NodeID {
			return a.Version < b.Version
		}
	}
	return false
}

func combineStates(a, b State) State {
	result := map[string]Entry{}
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		previous, existed := result[k]
		if existed {
			if !entryLess(previous, v) {
				continue
			}
		}
		result[k] = v
	}
	return result
}

func (s State) checkUpdated(addr string, entry Entry) bool {
	previous, existed := s[addr]
	if !existed {
		return true
	}
	return entryLess(previous, entry)
}

func uuidLess(a, b uuid.UUID) bool {
	for k := 0; k < len(a); k++ {
		if a[k] < b[k] {
			return true
		}
		if a[k] > b[k] {
			return false
		}
	}
	return false
}
