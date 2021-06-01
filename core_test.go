package crdtex

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestCoreService_Init__InitConn_And_UpdateRemote(t *testing.T) {
	t.Parallel()

	methods := &InterfaceMock{}
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
			WithSyncDuration(10*time.Second),
		),
	)
	syncTimer := &TimerMock{}
	s.syncTimer = syncTimer

	var syncDuration time.Duration
	syncTimer.ResetFunc = func(d time.Duration) {
		syncDuration = d
	}

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

	assert.Equal(t, 1, len(syncTimer.ResetCalls()))
	assert.Equal(t, 10*time.Second, syncDuration)

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

func mustParse(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestCoreService_Init__InitConn_And_UpdateRemote_ReturnNew(t *testing.T) {
	t.Parallel()

	methods := &InterfaceMock{}
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
			WithExpireDuration(30*time.Second),
		),
	)
	expireTimer := &TimerMock{}
	s.expireTimer = expireTimer

	var expireDuration time.Duration
	expireTimer.ResetFunc = func(d time.Duration) {
		expireDuration = d
	}

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

	assert.Equal(t, 1, len(expireTimer.ResetCalls()))
	assert.Equal(t, 30*time.Second, expireDuration)

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

func TestCoreService__Without_Remote_Addr__Update(t *testing.T) {
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

func TestCoreService_Update__Check_Expire_Timer(t *testing.T) {
	t.Parallel()

	methods := &InterfaceMock{}
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(WithExpireDuration(40*time.Second)),
	)

	expireTimer := &TimerMock{}
	s.expireTimer = expireTimer

	var expireDuration time.Duration
	expireTimer.ResetFunc = func(d time.Duration) {
		expireDuration = d
	}

	s.init(context.Background())

	assert.Equal(t, 0, len(expireTimer.ResetCalls()))

	//========================================================
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

	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:00Z") }
	s.run(context.Background())

	assert.Equal(t, 1, len(respChan))
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 1, len(expireTimer.ResetCalls()))
	assert.Equal(t, 40*time.Second, expireDuration)

	//========================================================
	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-2": {
				Seq:     8,
				NodeID:  uuid.MustParse("263fa371-ea35-4f2e-b1fa-fa4a346742bc"),
				Version: 30,
			},
		},
		respChan: respChan,
	}

	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:08Z") }
	s.run(context.Background())

	assert.Equal(t, 1, len(respChan))
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 2, len(expireTimer.ResetCalls()))
	assert.Equal(t, 32*time.Second, expireDuration)

	//========================================================
	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-1": {
				Seq:     12,
				NodeID:  uuid.MustParse("1a4aca71-835b-4af1-8056-5a2643316024"),
				Version: 21,
			},
		},
		respChan: respChan,
	}

	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:22Z") }
	s.run(context.Background())

	assert.Equal(t, 1, len(respChan))
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 3, len(expireTimer.ResetCalls()))
	assert.Equal(t, (48-22)*time.Second, expireDuration)
}

func TestCoreService_Init__Not_Reset_Expire(t *testing.T) {
	t.Parallel()

	methods := &InterfaceMock{}
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
			WithSyncDuration(10*time.Second),
		),
	)

	expireTimer := &TimerMock{}
	s.expireTimer = expireTimer

	expireTimer.ResetFunc = func(d time.Duration) {}

	methods.InitConnFunc = func(addr string) {}
	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state, nil
	}

	s.init(context.Background())

	assert.Equal(t, 0, len(expireTimer.ResetCalls()))

	assert.Equal(t, 1, len(methods.InitConnCalls()))
	assert.Equal(t, 1, len(methods.UpdateRemoteCalls()))
}
