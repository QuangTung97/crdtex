package crdtex

import (
	"context"
	"time"
)

const hundredYears = 100 * 365 * 24 * time.Hour

type updateResult struct {
	state State
	err   error
}

//go:generate moq -out core_mocks_test.go . callbacks

type callbacks interface {
	start(ctx context.Context, finish chan<- struct{})
	updateRemote(ctx context.Context, addr string, state State, resultChan chan<- updateResult)
}

type updateRequest struct {
	state    State
	respChan chan<- State
}

type coreService struct {
	methods callbacks
	self    nodeID
	options serviceOptions

	getNow      func() time.Time
	syncTimer   Timer
	expireTimer Timer

	finishChan chan struct{}
	cancel     func()

	// for outside in requests
	updateChan chan updateRequest
	// for inside out responses
	updateResultChan chan updateResult

	fetchLeaderChan chan fetchLeaderRequest

	state         State
	stateTerm     uint64
	stateVersion  uint64
	lastUpdate    map[string]time.Time
	nextAddrIndex int

	leader nodeID

	leaderWaitList  []chan<- string
	runnerIsRunning bool
}

type fetchLeaderRequest struct {
	lastLeader string
	respChan   chan<- string
}

type leaderWatcher struct {
	core *coreService
	ch   chan string
}

func newCoreService(
	methods callbacks, selfID nodeID, options serviceOptions,
) *coreService {
	finishChan := make(chan struct{}, 1)
	updateChan := make(chan updateRequest, 256)
	updateResultChan := make(chan updateResult, 16)
	fetchLeaderChan := make(chan fetchLeaderRequest, 128)
	return &coreService{
		methods: methods,
		self:    selfID,
		options: options,

		getNow:      func() time.Time { return time.Now() },
		syncTimer:   newTimer(),
		expireTimer: newTimer(),

		finishChan:       finishChan,
		updateChan:       updateChan,
		updateResultChan: updateResultChan,
		fetchLeaderChan:  fetchLeaderChan,

		lastUpdate:    map[string]time.Time{},
		nextAddrIndex: 0,
	}
}

func (s *coreService) checkAndCallResetExpireTimer(now time.Time, newState State) State {
	minAddr := ""
	minUpdate := now.AddDate(100, 0, 0)
	for addr, t := range s.lastUpdate {
		entry, ok := newState[addr]
		if !ok {
			panic("must be true")
		}

		if entry.OutOfSync {
			continue
		}

		if !t.Add(s.options.expireDuration).After(now) {
			entry.OutOfSync = true
			newState = newState.putEntry(addr, entry)
			continue
		}

		if minUpdate.After(t) {
			minUpdate = t
			minAddr = addr
		}
	}

	if minAddr != "" {
		s.expireTimer.Reset(minUpdate.Add(s.options.expireDuration).Sub(now))
	}
	return newState
}

func (s *coreService) updateWithState(inputState State) {
	now := s.getNow()

	newState := combineStates(s.state, inputState)
	for newAddr, newEntry := range newState {
		if newAddr == s.self.addr {
			continue
		}

		old, existed := s.state[newAddr]
		if !existed {
			s.lastUpdate[newAddr] = now
			continue
		}
		if old != newEntry {
			s.lastUpdate[newAddr] = now
		}
	}

	newState = s.checkAndCallResetExpireTimer(now, newState)
	s.state = newState
}

func (s *coreService) callUpdateRemote(ctx context.Context, addr string) {
	s.methods.updateRemote(ctx, addr, s.state, s.updateResultChan)
}

func (s *coreService) startLeader(ctx context.Context) {
	// TODO tests
	if !s.runnerIsRunning && s.leader == s.self {
		startCtx, cancel := context.WithCancel(ctx)
		s.cancel = cancel
		s.methods.start(startCtx, s.finishChan)
		s.runnerIsRunning = true
	}
}

func (s *coreService) computeAndStartLeader(ctx context.Context) {
	newLeader := s.state.computeLeader(
		s.self.addr, s.getNow().Add(-s.options.expireDuration), s.lastUpdate)

	// TODO only if running
	if s.leader == s.self && newLeader != s.self {
		s.cancel()
	}

	if s.leader.addr != newLeader.addr {
		for i, waiter := range s.leaderWaitList {
			waiter <- newLeader.addr
			s.leaderWaitList[i] = nil
		}
		s.leaderWaitList = s.leaderWaitList[:0]
	}

	s.leader = newLeader

	s.startLeader(ctx)
}

func (s *coreService) init(ctx context.Context) {
	s.syncTimer.Reset(s.options.syncDuration)

	s.stateTerm = 1
	s.stateVersion = 1
	newState := map[string]Entry{
		s.self.addr: {
			Term:      s.stateTerm,
			Timestamp: s.self.timestamp,
			Version:   s.stateVersion,
		},
	}
	s.state = newState

	for _, remoteAddr := range s.options.remoteAddresses {
		s.callUpdateRemote(ctx, remoteAddr)
	}
}

func (s *coreService) handleSyncTimerExpired(ctx context.Context) {
	s.stateVersion++
	newEntry := Entry{
		Term:      s.stateTerm,
		Timestamp: s.self.timestamp,
		Version:   s.stateVersion,
	}
	newTerm, updated := s.state.checkUpdated(s.self.addr, newEntry)
	if !updated {
		s.stateTerm = newTerm
		newEntry.Term = newTerm
	}
	s.state = s.state.putEntry(s.self.addr, newEntry)

	// TODO add test
	if len(s.options.remoteAddresses) > 0 {
		remoteAddr := s.options.remoteAddresses[s.nextAddrIndex]
		s.nextAddrIndex += (s.nextAddrIndex + 1) % len(s.options.remoteAddresses)
		s.callUpdateRemote(ctx, remoteAddr)
	}
	s.computeAndStartLeader(ctx)
}

func (s *coreService) run(ctx context.Context) {
	select {
	case req := <-s.updateChan:
		s.updateWithState(req.state)
		s.computeAndStartLeader(ctx)
		req.respChan <- s.state

	case <-s.syncTimer.Chan():
		s.syncTimer.ResetAfterChan(s.options.syncDuration)
		s.handleSyncTimerExpired(ctx)

	case <-s.expireTimer.Chan():
		s.expireTimer.ResetAfterChan(hundredYears)
		now := s.getNow()
		s.state = s.checkAndCallResetExpireTimer(now, s.state)
		s.computeAndStartLeader(ctx)

	case req := <-s.fetchLeaderChan:
		if req.lastLeader != s.leader.addr {
			req.respChan <- s.leader.addr
			return
		}
		s.leaderWaitList = append(s.leaderWaitList, req.respChan)

	case <-s.finishChan:
		s.runnerIsRunning = false
		s.startLeader(ctx)

	case <-ctx.Done():
		s.state = s.state.putEntry(s.self.addr, Entry{
			Term:      s.stateTerm,
			Timestamp: s.self.timestamp,
			Version:   s.stateVersion,
			OutOfSync: true,
		})
		for _, remoteAddr := range s.options.remoteAddresses {
			s.callUpdateRemote(context.Background(), remoteAddr)
		}
	}
}

func (s *coreService) getState() State {
	return s.state
}

func (s *coreService) fetchLeader(req fetchLeaderRequest) {
	s.fetchLeaderChan <- req
}

func (s *coreService) newLeaderWatcher() *leaderWatcher {
	ch := make(chan string, 1)
	return &leaderWatcher{
		core: s,
		ch:   ch,
	}
}

func (w *leaderWatcher) watch(lastLeader string) <-chan string {
	w.core.fetchLeader(fetchLeaderRequest{
		lastLeader: lastLeader,
		respChan:   w.ch,
	})
	return w.ch
}
