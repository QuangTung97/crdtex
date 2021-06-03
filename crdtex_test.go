package crdtex

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBoolLess(t *testing.T) {
	table := []struct {
		name     string
		a        bool
		b        bool
		expected bool
	}{
		{
			name:     "both-false",
			a:        false,
			b:        false,
			expected: false,
		},
		{
			name:     "a-true",
			a:        true,
			b:        false,
			expected: false,
		},
		{
			name:     "b-true",
			a:        false,
			b:        true,
			expected: true,
		},
		{
			name:     "both-true",
			a:        true,
			b:        true,
			expected: false,
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := boolLess(e.a, e.b)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestEntryLess(t *testing.T) {
	table := []struct {
		name     string
		a        Entry
		b        Entry
		expected bool
	}{
		{
			name:     "both-empty",
			expected: false,
		},
		{
			name: "a-term-greater",
			a: Entry{
				Term: 10,
			},
			b: Entry{
				Term: 9,
			},
			expected: false,
		},
		{
			name: "term-equal",
			a: Entry{
				Term: 10,
			},
			b: Entry{
				Term: 10,
			},
			expected: false,
		},
		{
			name: "a-term-less",
			a: Entry{
				Term: 10,
			},
			b: Entry{
				Term: 11,
			},
			expected: true,
		},
		{
			name: "a-term-less",
			a: Entry{
				Term: 10,
			},
			b: Entry{
				Term: 11,
			},
			expected: true,
		},
		{
			name: "timestamp-greater",
			a: Entry{
				Term:      10,
				Timestamp: 100,
			},
			b: Entry{
				Term:      10,
				Timestamp: 80,
			},
			expected: false,
		},
		{
			name: "timestamp-equal",
			a: Entry{
				Term:      10,
				Timestamp: 100,
			},
			b: Entry{
				Term:      10,
				Timestamp: 100,
			},
			expected: false,
		},
		{
			name: "timestamp-less",
			a: Entry{
				Term:      10,
				Timestamp: 100,
			},
			b: Entry{
				Term:      10,
				Timestamp: 120,
			},
			expected: true,
		},
		{
			name: "version-greater",
			a: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   21,
			},
			b: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
			},
			expected: false,
		},
		{
			name: "version-equal",
			a: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
			},
			b: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
			},
			expected: false,
		},
		{
			name: "version-less",
			a: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
			},
			b: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   22,
			},
			expected: true,
		},
		{
			name: "out-of-sync-greater",
			a: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
				OutOfSync: true,
			},
			b: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
				OutOfSync: false,
			},
			expected: false,
		},
		{
			name: "out-of-sync-equal",
			a: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
				OutOfSync: false,
			},
			b: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
				OutOfSync: false,
			},
			expected: false,
		},
		{
			name: "out-of-sync-less",
			a: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
				OutOfSync: false,
			},
			b: Entry{
				Term:      10,
				Timestamp: 100,
				Version:   20,
				OutOfSync: true,
			},
			expected: true,
		},
	}
	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := entryLess(e.a, e.b)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestCombineStates(t *testing.T) {
	table := []struct {
		name     string
		a        State
		b        State
		expected State
	}{
		{
			name:     "empty-with-empty",
			expected: map[string]Entry{},
		},
		{
			name: "a-non-empty",
			a: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 100,
					Version:   1,
				},
				"address2": {
					Term:      1,
					Timestamp: 80,
					Version:   2,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 100,
					Version:   1,
				},
				"address2": {
					Term:      1,
					Timestamp: 80,
					Version:   2,
				},
			},
		},
		{
			name: "b-non-empty",
			b: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 100,
					Version:   1,
				},
				"address2": {
					Term:      1,
					Timestamp: 80,
					Version:   2,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 100,
					Version:   1,
				},
				"address2": {
					Term:      1,
					Timestamp: 80,
					Version:   2,
				},
			},
		},
		{
			name: "a-b-different-keys",
			a: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 100,
					Version:   1,
				},
			},
			b: map[string]Entry{
				"address2": {
					Term:      1,
					Timestamp: 80,
					Version:   2,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 100,
					Version:   1,
				},
				"address2": {
					Term:      1,
					Timestamp: 80,
					Version:   2,
				},
			},
		},
		{
			name: "a-b-same-key-a-higher-term",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 120,
					Version:   1,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   10,
				},
			},
		},
		{
			name: "a-b-same-key-b-higher-term",
			a: map[string]Entry{
				"address1": {
					Term:      1,
					Timestamp: 120,
					Version:   1,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   10,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   10,
				},
			},
		},
		{
			name: "a-b-same-key-same-term-a-timestamp-less",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 120,
					Version:   11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 120,
					Version:   11,
				},
			},
		},
		{
			name: "a-b-same-key-same-term-b-timestamp-less",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 120,
					Version:   10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 120,
					Version:   10,
				},
			},
		},
		{
			name: "a-b-same-key-same-term-same-timestamp-a-version-less",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   11,
				},
			},
		},
		{
			name: "a-b-same-key-same-term-same-timestamp-a-version-greater",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
				},
			},
		},
		{
			name: "a-b-same-key-same-term-same-timestamp-same-version-a-out-of-sync-true",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: true,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: false,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: true,
				},
			},
		},
		{
			name: "a-b-same-key-same-term-same-timestamp-same-version-b-out-of-sync-true",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: false,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: true,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: true,
				},
			},
		},
		{
			name: "a-b-same-key-same-term-same-timestamp-same-version-out-of-sync-both-true",
			a: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: true,
				},
			},
			b: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: true,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Term:      2,
					Timestamp: 100,
					Version:   12,
					OutOfSync: true,
				},
			},
		},
	}
	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()
			result := combineStates(e.a, e.b)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestCheckUpdated(t *testing.T) {
	table := []struct {
		name     string
		state    State
		newAddr  string
		newEntry Entry
		newTerm  uint64
		updated  bool
	}{
		{
			name:    "entry-not-existed",
			newAddr: "address1",
			newEntry: Entry{
				Term:      1,
				Timestamp: 100,
				Version:   10,
			},
			updated: true,
		},
		{
			name: "entry-existed-smaller-timestamp-term-less",
			state: map[string]Entry{
				"address1": {
					Term:      5,
					Timestamp: 100,
					Version:   10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Term:      4,
				Timestamp: 80,
				Version:   20,
			},
			newTerm: 6,
			updated: false,
		},
		{
			name: "entry-existed-same-term-timestamp-less",
			state: map[string]Entry{
				"address1": {
					Term:      5,
					Timestamp: 100,
					Version:   10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Term:      5,
				Timestamp: 80,
				Version:   20,
			},
			newTerm: 6,
			updated: false,
		},
		{
			name: "entry-existed-term-less-timestamp-greater",
			state: map[string]Entry{
				"address1": {
					Term:      5,
					Timestamp: 100,
					Version:   10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Term:      4,
				Timestamp: 120,
				Version:   20,
			},
			newTerm: 5,
			updated: false,
		},
		{
			name: "entry-existed-same-timestamp-same-term-version-greater",
			state: map[string]Entry{
				"address1": {
					Term:      5,
					Timestamp: 100,
					Version:   10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Term:      5,
				Timestamp: 100,
				Version:   11,
			},
			updated: true,
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			newTerm, updated := e.state.checkUpdated(e.newAddr, e.newEntry)
			assert.Equal(t, e.updated, updated)
			assert.Equal(t, e.newTerm, newTerm)
		})
	}
}

func TestPutEntry(t *testing.T) {
	t.Parallel()

	s := State{
		"address-1": {
			Term:      4,
			Timestamp: 110,
			Version:   10,
			OutOfSync: false,
		},
	}

	result := s.putEntry("address-2", Entry{
		Term:      3,
		Timestamp: 80,
		Version:   5,
		OutOfSync: true,
	})
	assert.Equal(t, State{
		"address-1": {
			Term:      4,
			Timestamp: 110,
			Version:   10,
			OutOfSync: false,
		},
		"address-2": {
			Term:      3,
			Timestamp: 80,
			Version:   5,
			OutOfSync: true,
		},
	}, result)

	assert.Equal(t, State{
		"address-1": {
			Term:      4,
			Timestamp: 110,
			Version:   10,
			OutOfSync: false,
		},
	}, s)
}

func TestNodeIDLess(t *testing.T) {
	t.Parallel()

	table := []struct {
		name     string
		a        nodeID
		b        nodeID
		expected bool
	}{
		{
			name:     "both-empty",
			expected: false,
		},
		{
			name: "a-timestamp-greater",
			a: nodeID{
				timestamp: 100,
				addr:      "address1",
			},
			b: nodeID{
				timestamp: 80,
				addr:      "address2",
			},
			expected: false,
		},
		{
			name: "a-timestamp-equal",
			a: nodeID{
				timestamp: 100,
			},
			b: nodeID{
				timestamp: 100,
			},
			expected: false,
		},
		{
			name: "a-timestamp-less",
			a: nodeID{
				timestamp: 80,
			},
			b: nodeID{
				timestamp: 100,
			},
			expected: true,
		},
		{
			name: "addr-greater",
			a: nodeID{
				timestamp: 100,
				addr:      "address2",
			},
			b: nodeID{
				timestamp: 100,
				addr:      "address1",
			},
			expected: false,
		},
		{
			name: "addr-equal",
			a: nodeID{
				timestamp: 100,
				addr:      "address",
			},
			b: nodeID{
				timestamp: 100,
				addr:      "address",
			},
			expected: false,
		},
		{
			name: "addr-less",
			a: nodeID{
				timestamp: 100,
				addr:      "address1",
			},
			b: nodeID{
				timestamp: 100,
				addr:      "address2",
			},
			expected: true,
		},
	}
	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := nodeIDLess(e.a, e.b)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestComputeLeader(t *testing.T) {
	table := []struct {
		name       string
		selfAddr   string
		minTime    time.Time
		lastUpdate map[string]time.Time
		state      State

		expected nodeID
	}{
		{
			name:     "is-self-addr",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 100,
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			expected: nodeID{
				timestamp: 100,
				addr:      "address-1",
			},
		},
		{
			name:     "existing-node",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 120,
				},
				"address-2": {
					Timestamp: 100,
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-2": mustParse("2021-06-05T10:20:01Z"),
			},
			expected: nodeID{
				timestamp: 100,
				addr:      "address-2",
			},
		},
		{
			name:     "existing-node-out-of-sync",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 100,
				},
				"address-2": {
					Timestamp: 80,
					OutOfSync: true,
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-2": mustParse("2021-06-05T10:20:01Z"),
			},
			expected: nodeID{
				timestamp: 100,
				addr:      "address-1",
			},
		},
		{
			name:     "existing-nodes-no-last-update",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 140,
				},
				"address-2": {
					Timestamp: 120,
				},
				"address-3": {
					Timestamp: 100,
				},
			},
			minTime:    mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{},
			expected: nodeID{
				timestamp: 140,
				addr:      "address-1",
			},
		},
		{
			name:     "existing-nodes-with-last-update-greater",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 140,
				},
				"address-2": {
					Timestamp: 120,
				},
				"address-3": {
					Timestamp: 100,
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-3": mustParse("2021-06-05T10:20:01Z"),
			},
			expected: nodeID{
				timestamp: 100,
				addr:      "address-3",
			},
		},
		{
			name:     "existing-nodes-with-last-update-equals-min",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 140,
				},
				"address-2": {
					Timestamp: 120,
				},
				"address-3": {
					Timestamp: 100,
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-3": mustParse("2021-06-05T10:20:00Z"),
				"address-2": mustParse("2021-06-05T10:20:01Z"),
			},
			expected: nodeID{
				timestamp: 120,
				addr:      "address-2",
			},
		},
		{
			name:     "only-self-out-of-sync",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 100,
					Version:   10,
					OutOfSync: true,
				},
			},
			minTime:    mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{},
			expected: nodeID{
				timestamp: 100,
				addr:      "address-1",
			},
		},
		{
			name:     "same-timestamp",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					Timestamp: 100,
					Version:   10,
				},
				"address-2": {
					Timestamp: 100,
					Version:   10,
				},
				"address-0": {
					Timestamp: 100,
					Version:   10,
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-0": mustParse("2021-06-05T10:20:01Z"),
				"address-2": mustParse("2021-06-05T10:20:01Z"),
			},
			expected: nodeID{
				timestamp: 100,
				addr:      "address-0",
			},
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := e.state.computeLeader(e.selfAddr, e.minTime, e.lastUpdate)
			assert.Equal(t, e.expected, result)
		})
	}
}
