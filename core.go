package crdtex

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"time"
)

type updateRequest struct {
	state    State
	respChan chan<- State
}

type coreService struct {
	methods Interface

	selfAddr   string
	selfNodeID uuid.UUID
	options    serviceOptions

	getNow      func() time.Time
	syncTimer   Timer
	expireTimer Timer

	finishChan chan struct{}
	updateChan chan updateRequest

	state         State
	stateSeq      uint64
	stateVersion  uint64
	lastUpdate    map[string]time.Time
	nextAddrIndex int
}

func newCoreService(methods Interface, addr string, nodeID uuid.UUID, options serviceOptions) *coreService {
	finishChan := make(chan struct{}, 1)
	updateChan := make(chan updateRequest, 256)
	return &coreService{
		methods: methods,

		selfAddr:   addr,
		selfNodeID: nodeID,

		options: options,

		getNow:      func() time.Time { return time.Now() },
		syncTimer:   newTimer(),
		expireTimer: newTimer(),

		finishChan: finishChan,
		updateChan: updateChan,

		lastUpdate:    map[string]time.Time{},
		nextAddrIndex: 0,
	}
}

func (s *coreService) updateWithState(inputState State) {
	now := s.getNow()

	// TODO sync duration (checkUpdated)
	// TODO expire duration timer channel

	newState := combineStates(s.state, inputState)
	for newAddr, newEntry := range newState {
		if newAddr == s.selfAddr {
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

	minAddr := ""
	minUpdate := now.AddDate(100, 0, 0)
	for addr, t := range s.lastUpdate {
		if !t.Add(s.options.expireDuration).Before(now) && minUpdate.After(t) {
			minUpdate = t
			minAddr = addr
		}
	}

	if minAddr != "" {
		s.expireTimer.Reset(minUpdate.Add(s.options.expireDuration).Sub(now))
	}

	s.state = newState
}

func (s *coreService) callUpdateRemote(ctx context.Context, addr string) {
	ctx, cancel := context.WithTimeout(ctx, s.options.callRemoteTimeout)
	defer cancel()

	remoteState, err := s.methods.UpdateRemote(ctx, addr, s.state)
	if err != nil {
		// TODO logging
		fmt.Println("Error:", err)
		return
	}
	s.updateWithState(remoteState)
}

func (s *coreService) initAndCall(ctx context.Context, addr string) {
	s.methods.InitConn(addr)
	s.callUpdateRemote(ctx, addr)
}

func (s *coreService) init(ctx context.Context) {
	s.syncTimer.Reset(s.options.syncDuration)

	s.stateSeq = 1
	s.stateVersion = 1
	newState := map[string]Entry{
		s.selfAddr: {
			Seq:     s.stateSeq,
			NodeID:  s.selfNodeID,
			Version: s.stateVersion,
		},
	}
	s.state = newState

	for _, remoteAddr := range s.options.remoteAddresses {
		s.initAndCall(ctx, remoteAddr)
	}
}

func (s *coreService) run(ctx context.Context) {
	select {
	case req := <-s.updateChan:
		s.updateWithState(req.state)
		req.respChan <- s.state

	case <-s.syncTimer.Chan():
		s.syncTimer.ResetAfterChan(s.options.syncDuration)

		s.stateVersion++
		newEntry := Entry{
			Seq:     s.stateSeq,
			NodeID:  s.selfNodeID,
			Version: s.stateVersion,
		}
		newSeq, updated := s.state.checkUpdated(s.selfAddr, newEntry)
		if !updated {
			newEntry.Seq = newSeq
		}
		s.state = s.state.putEntry(s.selfAddr, newEntry)

		remoteAddr := s.options.remoteAddresses[s.nextAddrIndex]
		s.nextAddrIndex += (s.nextAddrIndex + 1) % len(s.options.remoteAddresses)
		s.callUpdateRemote(ctx, remoteAddr)
	}
}

func (s *coreService) getState() State {
	return s.state
}
