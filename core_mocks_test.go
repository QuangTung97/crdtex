// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package crdtex

import (
	"context"
	"sync"
)

// Ensure, that callbacksMock does implement callbacks.
// If this is not the case, regenerate this file with moq.
var _ callbacks = &callbacksMock{}

// callbacksMock is a mock implementation of callbacks.
//
// 	func TestSomethingThatUsescallbacks(t *testing.T) {
//
// 		// make and configure a mocked callbacks
// 		mockedcallbacks := &callbacksMock{
// 			startFunc: func(ctx context.Context, finish chan<- struct{})  {
// 				panic("mock out the start method")
// 			},
// 			updateRemoteFunc: func(ctx context.Context, addr string, state State, resultChan chan<- updateResult)  {
// 				panic("mock out the updateRemote method")
// 			},
// 		}
//
// 		// use mockedcallbacks in code that requires callbacks
// 		// and then make assertions.
//
// 	}
type callbacksMock struct {
	// startFunc mocks the start method.
	startFunc func(ctx context.Context, finish chan<- struct{})

	// updateRemoteFunc mocks the updateRemote method.
	updateRemoteFunc func(ctx context.Context, addr string, state State, resultChan chan<- updateResult)

	// calls tracks calls to the methods.
	calls struct {
		// start holds details about calls to the start method.
		start []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Finish is the finish argument value.
			Finish chan<- struct{}
		}
		// updateRemote holds details about calls to the updateRemote method.
		updateRemote []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// Addr is the addr argument value.
			Addr string
			// State is the state argument value.
			State State
			// ResultChan is the resultChan argument value.
			ResultChan chan<- updateResult
		}
	}
	lockstart        sync.RWMutex
	lockupdateRemote sync.RWMutex
}

// start calls startFunc.
func (mock *callbacksMock) start(ctx context.Context, finish chan<- struct{}) {
	if mock.startFunc == nil {
		panic("callbacksMock.startFunc: method is nil but callbacks.start was just called")
	}
	callInfo := struct {
		Ctx    context.Context
		Finish chan<- struct{}
	}{
		Ctx:    ctx,
		Finish: finish,
	}
	mock.lockstart.Lock()
	mock.calls.start = append(mock.calls.start, callInfo)
	mock.lockstart.Unlock()
	mock.startFunc(ctx, finish)
}

// startCalls gets all the calls that were made to start.
// Check the length with:
//     len(mockedcallbacks.startCalls())
func (mock *callbacksMock) startCalls() []struct {
	Ctx    context.Context
	Finish chan<- struct{}
} {
	var calls []struct {
		Ctx    context.Context
		Finish chan<- struct{}
	}
	mock.lockstart.RLock()
	calls = mock.calls.start
	mock.lockstart.RUnlock()
	return calls
}

// updateRemote calls updateRemoteFunc.
func (mock *callbacksMock) updateRemote(ctx context.Context, addr string, state State, resultChan chan<- updateResult) {
	if mock.updateRemoteFunc == nil {
		panic("callbacksMock.updateRemoteFunc: method is nil but callbacks.updateRemote was just called")
	}
	callInfo := struct {
		Ctx        context.Context
		Addr       string
		State      State
		ResultChan chan<- updateResult
	}{
		Ctx:        ctx,
		Addr:       addr,
		State:      state,
		ResultChan: resultChan,
	}
	mock.lockupdateRemote.Lock()
	mock.calls.updateRemote = append(mock.calls.updateRemote, callInfo)
	mock.lockupdateRemote.Unlock()
	mock.updateRemoteFunc(ctx, addr, state, resultChan)
}

// updateRemoteCalls gets all the calls that were made to updateRemote.
// Check the length with:
//     len(mockedcallbacks.updateRemoteCalls())
func (mock *callbacksMock) updateRemoteCalls() []struct {
	Ctx        context.Context
	Addr       string
	State      State
	ResultChan chan<- updateResult
} {
	var calls []struct {
		Ctx        context.Context
		Addr       string
		State      State
		ResultChan chan<- updateResult
	}
	mock.lockupdateRemote.RLock()
	calls = mock.calls.updateRemote
	mock.lockupdateRemote.RUnlock()
	return calls
}
