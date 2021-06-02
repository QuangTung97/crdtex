package crdtex

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newInterfaceMock() *InterfaceMock {
	methods := &InterfaceMock{}
	methods.InitConnFunc = func(addr string) {}
	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state, nil
	}
	methods.StartFunc = func(ctx context.Context, finish chan<- struct{}) {}
	return methods
}

func TestCoreService_Init__InitConn_And_UpdateRemote(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
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

	methods := newInterfaceMock()
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

	methods := newInterfaceMock()
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

	methods := newInterfaceMock()
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
	// With different address
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
	// Same address as existing
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

	methods := newInterfaceMock()
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

	s.init(context.Background())

	assert.Equal(t, 0, len(expireTimer.ResetCalls()))

	assert.Equal(t, 1, len(methods.InitConnCalls()))
	assert.Equal(t, 1, len(methods.UpdateRemoteCalls()))
}

func TestCoreService_Update__With_Self_Addr__Not_Reset_Expire(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(WithExpireDuration(40*time.Second)),
	)

	expireTimer := &TimerMock{}
	s.expireTimer = expireTimer

	expireTimer.ResetFunc = func(d time.Duration) {}

	s.init(context.Background())

	assert.Equal(t, 0, len(expireTimer.ResetCalls()))

	//========================================================
	respChan := make(chan State, 1)
	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"self-addr": {
				Seq:     1,
				NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
				Version: 2,
			},
		},
		respChan: respChan,
	}

	s.run(context.Background())

	assert.Equal(t, 1, len(respChan))
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 0, len(expireTimer.ResetCalls()))
}

func TestCoreService_Update__With_Expired_Addr(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
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
				Seq:     1,
				NodeID:  uuid.MustParse("aa42c5b8-2a2c-4416-87d5-54e1e532fd39"),
				Version: 2,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:00Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-2": {
				Seq:     1,
				NodeID:  uuid.MustParse("66c8bd3e-3750-436c-a54a-a8e9a353f2bd"),
				Version: 2,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:40Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 2, len(expireTimer.ResetCalls()))
	assert.Equal(t, 40*time.Second, expireDuration)
}

func TestCoreService_Update__With_Nearly_Expired_Addr(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
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
				Seq:     1,
				NodeID:  uuid.MustParse("aa42c5b8-2a2c-4416-87d5-54e1e532fd39"),
				Version: 2,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:00Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-2": {
				Seq:     1,
				NodeID:  uuid.MustParse("66c8bd3e-3750-436c-a54a-a8e9a353f2bd"),
				Version: 2,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:39Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 2, len(expireTimer.ResetCalls()))
	assert.Equal(t, 1*time.Second, expireDuration)
}

func TestCoreService_Update__With_Out_Of_Sync_New_Node(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
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
				Seq:     1,
				NodeID:  uuid.MustParse("aa42c5b8-2a2c-4416-87d5-54e1e532fd39"),
				Version: 2,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:00Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-2": {
				Seq:       1,
				NodeID:    uuid.MustParse("66c8bd3e-3750-436c-a54a-a8e9a353f2bd"),
				Version:   2,
				OutOfSync: true,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:42Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 1, len(expireTimer.ResetCalls()))
	assert.Equal(t, 40*time.Second, expireDuration)
}

func TestCoreService_Update__With_Out_Of_Sync__First_Update(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
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
				Seq:       1,
				NodeID:    uuid.MustParse("aa42c5b8-2a2c-4416-87d5-54e1e532fd39"),
				Version:   2,
				OutOfSync: true,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:00Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 0, len(expireTimer.ResetCalls()))

	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-1": {
				Seq:     1,
				NodeID:  uuid.MustParse("aa42c5b8-2a2c-4416-87d5-54e1e532fd39"),
				Version: 2,
			},
			"remote-addr-2": {
				Seq:     1,
				NodeID:  uuid.MustParse("66c8bd3e-3750-436c-a54a-a8e9a353f2bd"),
				Version: 2,
			},
		},
		respChan: respChan,
	}
	s.getNow = func() time.Time { return mustParse("2021-06-05T10:20:22Z") }
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	assert.Equal(t, 1, len(expireTimer.ResetCalls()))
	assert.Equal(t, 40*time.Second, expireDuration)
}

func TestCoreService_Sync_Call_Single_Remote(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()

	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
			WithSyncDuration(3*time.Second),
		))

	var updateAddr string
	var updateState State
	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		updateAddr = addr
		updateState = state
		return state, nil
	}

	syncTimer := &TimerMock{}
	s.syncTimer = syncTimer

	syncTimer.ResetFunc = func(d time.Duration) {}

	s.init(context.Background())

	assert.Equal(t, 1, len(methods.UpdateRemoteCalls()))
	var expected State = map[string]Entry{
		"self-addr": {
			Seq:     1,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 1,
		},
	}
	assert.Equal(t, expected, updateState)

	// Update
	syncTimer.ChanFunc = func() <-chan time.Time {
		return nil
	}

	respChan := make(chan State, 1)
	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"self-addr": {
				Seq:     10,
				NodeID:  uuid.MustParse("6a4aca71-835b-4af1-8056-5a2643316024"),
				Version: 20,
			},
		},
		respChan: respChan,
	}
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	ch := make(chan time.Time, 1)

	var syncResetDuration time.Duration
	syncTimer.ResetAfterChanFunc = func(d time.Duration) {
		syncResetDuration = d
	}

	// Sync Expire 1
	ch <- time.Now()
	syncTimer.ChanFunc = func() <-chan time.Time {
		return ch
	}

	s.run(context.Background())

	assert.Equal(t, 1, len(syncTimer.ResetAfterChanCalls()))
	assert.Equal(t, 3*time.Second, syncResetDuration)

	assert.Equal(t, 2, len(methods.UpdateRemoteCalls()))
	expected = map[string]Entry{
		"self-addr": {
			Seq:     11,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 2,
		},
	}
	assert.Equal(t, "remote-addr-1", updateAddr)
	assert.Equal(t, expected, updateState)

	// Sync Expire 2
	ch <- time.Now()
	s.run(context.Background())

	assert.Equal(t, 2, len(syncTimer.ResetAfterChanCalls()))
	assert.Equal(t, 3*time.Second, syncResetDuration)

	assert.Equal(t, 3, len(methods.UpdateRemoteCalls()))
	expected = map[string]Entry{
		"self-addr": {
			Seq:     11,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 3,
		},
	}
	assert.Equal(t, "remote-addr-1", updateAddr)
	assert.Equal(t, expected, updateState)
}

func TestCoreService_Sync_Call_Two_Remotes(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()

	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
			AddRemoteAddress("remote-addr-2"),
			WithSyncDuration(3*time.Second),
		))

	var updateAddr string
	var updateState State
	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		updateAddr = addr
		updateState = state
		return state, nil
	}

	syncTimer := &TimerMock{}
	s.syncTimer = syncTimer

	syncTimer.ResetFunc = func(d time.Duration) {}

	s.init(context.Background())

	assert.Equal(t, 2, len(methods.UpdateRemoteCalls()))
	var expected State = map[string]Entry{
		"self-addr": {
			Seq:     1,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 1,
		},
	}
	assert.Equal(t, expected, updateState)

	// Update
	syncTimer.ChanFunc = func() <-chan time.Time {
		return nil
	}

	respChan := make(chan State, 1)
	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"self-addr": {
				Seq:     10,
				NodeID:  uuid.MustParse("6a4aca71-835b-4af1-8056-5a2643316024"),
				Version: 20,
			},
		},
		respChan: respChan,
	}
	s.run(context.Background())
	drainUpdateResponseChan(respChan)

	ch := make(chan time.Time, 1)

	var syncResetDuration time.Duration
	syncTimer.ResetAfterChanFunc = func(d time.Duration) {
		syncResetDuration = d
	}

	// Sync Expire 1
	ch <- time.Now()
	syncTimer.ChanFunc = func() <-chan time.Time {
		return ch
	}

	s.run(context.Background())

	assert.Equal(t, 1, len(syncTimer.ResetAfterChanCalls()))
	assert.Equal(t, 3*time.Second, syncResetDuration)

	assert.Equal(t, 3, len(methods.UpdateRemoteCalls()))
	expected = map[string]Entry{
		"self-addr": {
			Seq:     11,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 2,
		},
	}
	assert.Equal(t, "remote-addr-1", updateAddr)
	assert.Equal(t, expected, updateState)

	// Sync Expire 2
	ch <- time.Now()
	s.run(context.Background())

	assert.Equal(t, 2, len(syncTimer.ResetAfterChanCalls()))
	assert.Equal(t, 3*time.Second, syncResetDuration)

	assert.Equal(t, 4, len(methods.UpdateRemoteCalls()))
	expected = map[string]Entry{
		"self-addr": {
			Seq:     11,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 3,
		},
	}
	assert.Equal(t, "remote-addr-2", updateAddr)
	assert.Equal(t, expected, updateState)
}

func TestCoreService_Init__Only_Node__Start_Runner(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	var startCtx context.Context
	methods.StartFunc = func(ctx context.Context, finish chan<- struct{}) {
		startCtx = ctx
	}

	s.init(context.Background())

	assert.Equal(t, 1, len(methods.StartCalls()))
	assert.NotEqual(t, context.Background(), startCtx)
}

func TestCoreService_Init__Have_Existing_Node_With_Bigger_ID__Start_Runner(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state.putEntry("remote-addr-1", Entry{
			Seq:     1,
			NodeID:  uuid.MustParse("693cb116-6b95-4d8e-893f-86106185b638"),
			Version: 1,
		}), nil
	}

	s.init(context.Background())

	assert.Equal(t, 1, len(methods.StartCalls()))
}

func TestCoreService_Init__Have_Existing_Node_With_Smaller_ID__Not_Start_Runner(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state.putEntry("remote-addr-1", Entry{
			Seq:     1,
			NodeID:  uuid.MustParse("493cb116-6b95-4d8e-893f-86106185b638"),
			Version: 1,
		}), nil
	}

	s.init(context.Background())

	assert.Equal(t, 0, len(methods.StartCalls()))
}

func TestCoreService_WatchLeader_First_Time(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state, nil
	}

	s.init(context.Background())

	watcher := s.newLeaderWatcher()
	leaderCh := watcher.watch("")

	s.run(context.Background())

	leaderAddr := <-leaderCh
	assert.Equal(t, "self-addr", leaderAddr)
}

func TestCoreService_WatchLeader_Second_Time(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state, nil
	}

	s.init(context.Background())

	watcher := s.newLeaderWatcher()

	leaderCh := watcher.watch("")
	s.run(context.Background())
	lastLeader := <-leaderCh

	leaderCh = watcher.watch(lastLeader)
	s.run(context.Background())

	assert.Equal(t, 0, len(leaderCh))
}

func drainLeaderChan(ch <-chan string) string {
	select {
	case s := <-ch:
		return s
	default:
		return ""
	}
}

func TestCoreService_WatchLeader_Second_Times_After_Leader_Change(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	var startCtx context.Context
	methods.StartFunc = func(ctx context.Context, finish chan<- struct{}) {
		startCtx = ctx
	}

	s.init(context.Background())

	watcher := s.newLeaderWatcher()

	leaderCh := watcher.watch("")
	s.run(context.Background())
	lastLeader := <-leaderCh

	leaderCh = watcher.watch(lastLeader)
	s.run(context.Background())
	assert.Equal(t, 0, len(leaderCh))

	updateRespCh := make(chan State, 1)
	s.updateChan <- updateRequest{
		state: map[string]Entry{
			"remote-addr-1": {
				Seq:     1,
				NodeID:  uuid.MustParse("429993d4-3e77-48f8-b2f8-4e4d679e8645"),
				Version: 1,
			},
		},
		respChan: updateRespCh,
	}
	s.run(context.Background())

	assert.Equal(t, 1, len(updateRespCh))
	respState := drainUpdateResponseChan(updateRespCh)
	assert.Equal(t, State{
		"self-addr": {
			Seq:     1,
			NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 1,
		},
		"remote-addr-1": {
			Seq:     1,
			NodeID:  uuid.MustParse("429993d4-3e77-48f8-b2f8-4e4d679e8645"),
			Version: 1,
		},
	}, respState)
	assert.Equal(t, 1, len(methods.StartCalls()))
	assert.Equal(t, context.Canceled, startCtx.Err())

	assert.Equal(t, 1, len(leaderCh))
	assert.Equal(t, "remote-addr-1", drainLeaderChan(leaderCh))
	assert.Equal(t, 0, len(s.leaderWaitList))
}

func TestCoreService_Update_Empty__Only_Start_Once(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	s.init(context.Background())

	respCh := make(chan State, 1)
	s.updateChan <- updateRequest{
		state:    map[string]Entry{},
		respChan: respCh,
	}
	s.run(context.Background())
	drainUpdateResponseChan(respCh)

	assert.Equal(t, 1, len(methods.StartCalls()))
}

func TestCoreService_Finish__While_Still_The_Leader__Start_Again(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)
	var finishCh chan<- struct{}
	methods.StartFunc = func(ctx context.Context, finish chan<- struct{}) {
		finishCh = finish
	}

	s.init(context.Background())
	assert.Equal(t, 1, len(methods.StartCalls()))

	finishCh <- struct{}{}
	s.run(context.Background())
	assert.Equal(t, 2, len(methods.StartCalls()))
}

func TestCoreService__Sync_Timer_Expire__Remote_Out_Of_Sync__Start_Runner(t *testing.T) {
	t.Parallel()

	methods := newInterfaceMock()
	id := uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7")
	s := newCoreService(methods, "self-addr", id,
		computeOptions(
			AddRemoteAddress("remote-addr-1"),
		),
	)

	syncTimer := &TimerMock{}
	s.syncTimer = syncTimer

	syncTimer.ResetFunc = func(d time.Duration) {}
	syncTimer.ResetAfterChanFunc = func(d time.Duration) {}

	timerCh := make(chan time.Time, 1)
	timerCh <- time.Now()
	syncTimer.ChanFunc = func() <-chan time.Time {
		return timerCh
	}

	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state.putEntry("remote-addr-1", Entry{
			Seq:     1,
			NodeID:  uuid.MustParse("435dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version: 1,
		}), nil
	}

	s.init(context.Background())

	methods.UpdateRemoteFunc = func(ctx context.Context, addr string, state State) (State, error) {
		return state.putEntry("remote-addr-1", Entry{
			Seq:       1,
			NodeID:    uuid.MustParse("435dbd7a-9a65-48b3-8644-0fb58eed98d7"),
			Version:   1,
			OutOfSync: true,
		}), nil
	}

	s.run(context.Background())

	assert.Equal(t, 2, len(methods.UpdateRemoteCalls()))
	assert.Equal(t, 1, len(methods.StartCalls()))
}
