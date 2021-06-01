package crdtex

import (
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

func combineStates(a, b State) State {
	result := map[string]Entry{}
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		previous, existed := result[k]
		if existed {
			if previous.Seq > v.Seq {
				continue
			}
			if previous.Seq == v.Seq && !uuidLess(previous.NodeID, v.NodeID) {
				continue
			}
		}
		result[k] = v
	}
	return result
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
