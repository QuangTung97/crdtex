package crdtex

import (
	"context"
	"sort"
	"time"
)

// Entry ...
type Entry struct {
	Term      uint64
	Timestamp uint64
	Version   uint64
	OutOfSync bool
}

// State ...
type State map[string]Entry

// TODO Start run on a goroutine
// TODO Async Update Remote, on a goroutine

// Interface ...
type Interface interface {
	Start(ctx context.Context)
	UpdateRemote(ctx context.Context, addr string, state State) (State, error)
}

//go:generate moq -out crdtex_mocks_test.go . Timer

// Timer for timer
type Timer interface {
	Reset(d time.Duration)
	ResetAfterChan(d time.Duration)
	Chan() <-chan time.Time
}

// LeaderWatcher ...
type LeaderWatcher struct {
	coreWatcher *leaderWatcher
	lastLeader  string
}

// Runner ...
type Runner struct {
	core *coreService
}

// NewRunner creates a Runner
func NewRunner(_ Interface, selfAddr string, options ...Option) *Runner {
	timestamp := time.Now().UnixNano()
	// TODO nil
	self := nodeID{
		timestamp: uint64(timestamp),
		addr:      selfAddr,
	}
	core := newCoreService(nil, self, computeOptions(options...))
	return &Runner{
		core: core,
	}
}

// Run ...
func (r *Runner) Run(ctx context.Context) {
	r.core.init(ctx)
	if ctx.Err() != nil {
		return
	}

	for {
		r.core.run(ctx)
		if ctx.Err() != nil {
			return
		}
	}
}

// Update ...
func (r *Runner) Update(ctx context.Context, state State) State {
	respChan := make(chan State, 1)
	r.core.updateChan <- updateRequest{
		state:    state,
		respChan: respChan,
	}
	select {
	case result := <-respChan:
		return result
	case <-ctx.Done():
		return nil
	}
}

// NewLeaderWatcher creates a watcher
func (r *Runner) NewLeaderWatcher() *LeaderWatcher {
	return &LeaderWatcher{
		coreWatcher: r.core.newLeaderWatcher(),
		lastLeader:  "",
	}
}

// Watch ...
func (w *LeaderWatcher) Watch(ctx context.Context) string {
	ch := w.coreWatcher.watch(w.lastLeader)
	select {
	case leader := <-ch:
		w.lastLeader = leader
		return leader

	case <-ctx.Done():
		return ""
	}
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
	if a.Term < b.Term {
		return true
	}
	if a.Term > b.Term {
		return false
	}

	if a.Timestamp < b.Timestamp {
		return true
	}
	if a.Timestamp > b.Timestamp {
		return false
	}

	if a.Version < b.Version {
		return true
	}
	if a.Version > b.Version {
		return false
	}

	return boolLess(a.OutOfSync, b.OutOfSync)
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

	seq := previous.Term
	newEntry := entry
	newEntry.Term = seq

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

type nodeID struct {
	timestamp uint64
	addr      string
}

func nodeIDLess(a, b nodeID) bool {
	if a.timestamp < b.timestamp {
		return true
	}
	if a.timestamp > b.timestamp {
		return false
	}
	return a.addr < b.addr
}

type sortNodeID []nodeID

var _ sort.Interface = sortNodeID{}

func (s sortNodeID) Len() int {
	return len(s)
}

func (s sortNodeID) Less(i, j int) bool {
	return nodeIDLess(s[i], s[j])
}

func (s sortNodeID) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

func (s State) computeLeader(selfAddr string, minTime time.Time, lastUpdate map[string]time.Time) nodeID {
	var nodeIDs []nodeID
	nodeIDs = append(nodeIDs, nodeID{
		timestamp: s[selfAddr].Timestamp,
		addr:      selfAddr,
	})

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
			nodeIDs = append(nodeIDs, nodeID{
				timestamp: e.Timestamp,
				addr:      addr,
			})
		}
	}
	sort.Sort(sortNodeID(nodeIDs))
	return nodeIDs[0]
}
