package crdtex

import (
	"context"
	"github.com/google/uuid"
	"time"
)

// Entry ...
type Entry struct {
	Seq     uint64
	NodeID  uuid.UUID
	Version uint64
}

// State ...
type State map[string]Entry

//go:generate moq -out crdtex_mocks_test.go . Interface Timer

// Interface ...
type Interface interface {
	Start(ctx context.Context, finish chan<- struct{})
	InitConn(addr string)
	UpdateRemote(ctx context.Context, addr string, state State) (State, error)
}

// Timer for timer
type Timer interface {
	Reset(d time.Duration)
	ResetAfterChan(d time.Duration)
	Chan() <-chan time.Time
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

func (s State) checkUpdated(addr string, entry Entry) (uint64, bool) {
	previous, existed := s[addr]
	if !existed {
		return 0, true
	}
	updated := entryLess(previous, entry)
	if updated {
		return 0, true
	}

	seq := previous.Seq
	newEntry := entry
	newEntry.Seq = seq

	if entryLess(previous, newEntry) {
		return seq, false
	}
	return seq + 1, false
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
