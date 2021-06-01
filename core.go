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

	selfAddr          string
	selfNodeID        uuid.UUID
	remoteAddresses   []string
	callRemoteTimeout time.Duration

	finishChan chan struct{}
	updateChan chan updateRequest

	state State
}

func newCoreService(methods Interface, addr string, nodeID uuid.UUID, options serviceOptions) *coreService {
	finishChan := make(chan struct{}, 1)
	updateChan := make(chan updateRequest, 256)
	return &coreService{
		methods: methods,

		selfAddr:          addr,
		selfNodeID:        nodeID,
		remoteAddresses:   options.remoteAddresses,
		callRemoteTimeout: options.callRemoteTimeout,

		finishChan: finishChan,
		updateChan: updateChan,
	}
}

func (s *coreService) initAndCall(ctx context.Context, addr string) {
	s.methods.InitConn(addr)
	ctx, cancel := context.WithTimeout(ctx, s.callRemoteTimeout)
	defer cancel()

	remoteState, err := s.methods.UpdateRemote(ctx, addr, s.state)
	if err != nil {
		// TODO logging
		fmt.Println("Error:", err)
		return
	}
	s.state = combineStates(s.state, remoteState)
}

func (s *coreService) init(ctx context.Context) {
	newState := map[string]Entry{}
	for k, v := range s.state {
		newState[k] = v
	}
	newState[s.selfAddr] = Entry{
		Seq:     1,
		NodeID:  s.selfNodeID,
		Version: 1,
	}
	s.state = newState

	for _, remoteAddr := range s.remoteAddresses {
		s.initAndCall(ctx, remoteAddr)
	}
}

func (s *coreService) run(ctx context.Context) {
	select {
	case req := <-s.updateChan:
		s.state = combineStates(s.state, req.state)
		req.respChan <- s.state
	}
}

func (s *coreService) getState() State {
	return s.state
}
