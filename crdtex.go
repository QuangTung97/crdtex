package crdtex

import (
	"context"
	"github.com/google/uuid"
	"sort"
	"time"
)

// Entry ...
type Entry struct {
	Seq       uint64
	NodeID    uuid.UUID
	Version   uint64
	OutOfSync bool
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

func boolLess(a, b bool) bool {
	if a == b {
		return false
	}
	if b == true {
		return true
	}
	return false
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
			if a.Version < b.Version {
				return true
			}
			if a.Version == b.Version {
				return boolLess(a.OutOfSync, b.OutOfSync)
			}
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

func (s State) putEntry(addr string, entry Entry) State {
	result := map[string]Entry{}
	for k, v := range s {
		result[k] = v
	}
	result[addr] = entry
	return result
}

type sortUUID []uuid.UUID

var _ sort.Interface = sortUUID{}

func (s sortUUID) Len() int {
	return len(s)
}

func (s sortUUID) Less(i, j int) bool {
	return uuidLess(s[i], s[j])
}

func (s sortUUID) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

func (s State) computeLeader(selfAddr string, minTime time.Time, lastUpdate map[string]time.Time) uuid.UUID {
	var idList []uuid.UUID
	idList = append(idList, s[selfAddr].NodeID)

	for addr, e := range s {
		if addr == selfAddr {
			continue
		}
		if e.OutOfSync {
			continue
		}
		lastTime, ok := lastUpdate[addr]
		if !ok {
			continue
		}

		// now >= t + 30 => false
		// now - 30 >= t => false
		// t > now - 30 => true
		if lastTime.After(minTime) {
			idList = append(idList, e.NodeID)
		}
	}
	sort.Sort(sortUUID(idList))
	return idList[0]
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
