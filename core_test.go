package crdtex

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCoreService_Init__InitConn_And_UpdateRemote(t *testing.T) {
	t.Parallel()

	methods := &InterfaceMock{}
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	var initAddr string
	methods.InitConnFunc = func(addr string) {
		initAddr = addr
	}

	var updateAddr string
	var updateState State
	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		updateAddr = addr
		updateState = state
		return state, nil
	}

	s.init(context.Background())

	assert.Equal(t, 1, len(methods.InitConnCalls()))
	assert.Equal(t, "remote-addr-1", initAddr)

	assert.Equal(t, 1, len(methods.UpdateRemoteCalls()))
	assert.Equal(t, "remote-addr-1", updateAddr)
	var expected State = map[string]Entry{
		"self-addr": {
			Seq:     1,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 1,
		},
	}
	assert.Equal(t, expected, updateState)
}

func TestCoreService_Init__InitConn_And_UpdateRemote_ReturnNew(t *testing.T) {
	t.Parallel()

	methods := &InterfaceMock{}
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	methods.InitConnFunc = func(addr string) {
	}

	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return map[string]Entry{
			"self-addr": {
				Seq:     1,
				NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
				Version: 1,
			},
			"remote-addr-1": {
				Seq:     1,
				NodeID:  uuid.MustParse("1a4aca71-835b-4af1-8056-5a2643316024"),
				Version: 10,
			},
		}, nil
	}

	s.init(context.Background())

	assert.Equal(t, 1, len(methods.InitConnCalls()))
	assert.Equal(t, 1, len(methods.UpdateRemoteCalls()))

	var expected State = map[string]Entry{
		"self-addr": {
			Seq:     1,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 1,
		},
		"remote-addr-1": {
			Seq:     1,
			NodeID:  uuid.MustParse("1a4aca71-835b-4af1-8056-5a2643316024"),
			Version: 10,
		},
	}
	assert.Equal(t, expected, s.getState())
}

func drainUpdateResponseChan(ch <-chan State) State {
	select {
	case s := <-ch:
		return s
	default:
		return nil
	}
}

func TestCoreService_Update_Without_Remote_Addr(t *testing.T) {
	t.Parallel()

	methods := &InterfaceMock{}
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id, computeOptions())

	s.init(context.Background())

	respChan := make(chan State, 1)
	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-1": {
				Seq:     12,
				NodeID:  uuid.MustParse("1a4aca71-835b-4af1-8056-5a2643316024"),
				Version: 20,
			},
		},
		respChan: respChan,
	}

	s.run(context.Background())
	assert.Equal(t, 1, len(respChan))
	result := drainUpdateResponseChan(respChan)

	var expected State = map[string]Entry{
		"self-addr": {
			Seq:     1,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 1,
		},
		"remote-addr-1": {
			Seq:     12,
			NodeID:  uuid.MustParse("1a4aca71-835b-4af1-8056-5a2643316024"),
			Version: 20,
		},
	}
	assert.Equal(t, expected, result)
	assert.Equal(t, expected, s.getState())
}
