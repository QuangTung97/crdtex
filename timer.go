package crdtex

import "time"

type simpleTimer struct {
	timer *time.Timer
}

var _ Timer = simpleTimer{}

func newTimer() simpleTimer {
	return simpleTimer{
		timer: time.NewTimer(100 * 365 * 24 * time.Hour),
	}
}

// Reset resets the timer
func (t simpleTimer) Reset(d time.Duration) {
	if !t.timer.Stop() {
		<-t.timer.C
	}
	t.timer.Reset(d)
}

// ResetAfterChan resets the timer right after receive from channel
func (t simpleTimer) ResetAfterChan(d time.Duration) {
	t.timer.Reset(d)
}

// Chan returns the timer channel
func (t simpleTimer) Chan() <-chan time.Time {
	return t.timer.C
}
