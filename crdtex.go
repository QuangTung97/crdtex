package crdtex

import (
	"github.com/google/uuid"
	"sort"
	"time"
)

// Entry ...
type Entry struct {
	Addr       string
	Version    uint64
	Deleted    bool
	LastUpdate time.Time
}

// State ...
type State map[uuid.UUID]Entry

func combineStates(a, b State) State {
	result := map[uuid.UUID]Entry{}
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		prev, existed := result[k]
		if existed {
			if prev.Version < v.Version {
				result[k] = v
			}
		} else {
			result[k] = v
		}
	}
	return result
}

func computeAddressSet(s State) []string {
	set := map[string]struct{}{}
	for _, e := range s {
		set[e.Addr] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for e := range set {
		result = append(result, e)
	}
	sort.Strings(result)
	return result
}
