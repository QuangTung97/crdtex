package crdtex

import (
	"github.com/stretchr/testify/assert"
	"testing"
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

//
//func TestCheckUpdated(t *testing.T) {
//	table := []struct {
//		name     string
//		state    State
//		newAddr  string
//		newEntry Entry
//		newSeq   uint64
//		updated  bool
//	}{
//		{
//			name:    "entry-not-existed",
//			newAddr: "address1",
//			newEntry: Entry{
//				Seq:     1,
//				NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//				Version: 10,
//			},
//			updated: true,
//		},
//		{
//			name: "entry-existed-different-id-seq-less",
//			state: map[string]Entry{
//				"address1": {
//					Seq:     5,
//					NodeID:  uuid.MustParse("cda641f5-0770-4ef7-9d58-0d6b3e75a355"),
//					Version: 10,
//				},
//			},
//			newAddr: "address1",
//			newEntry: Entry{
//				Seq:     4,
//				NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//				Version: 20,
//			},
//			newSeq:  6,
//			updated: false,
//		},
//		{
//			name: "entry-existed-same-seq-id-less",
//			state: map[string]Entry{
//				"address1": {
//					Seq:     5,
//					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//					Version: 10,
//				},
//			},
//			newAddr: "address1",
//			newEntry: Entry{
//				Seq:     5,
//				NodeID:  uuid.MustParse("a1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//				Version: 20,
//			},
//			newSeq:  6,
//			updated: false,
//		},
//		{
//			name: "entry-existed-seq-less-id-greater",
//			state: map[string]Entry{
//				"address1": {
//					Seq:     5,
//					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//					Version: 10,
//				},
//			},
//			newAddr: "address1",
//			newEntry: Entry{
//				Seq:     4,
//				NodeID:  uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//				Version: 20,
//			},
//			newSeq:  5,
//			updated: false,
//		},
//		{
//			name: "entry-existed-same-id-same-seq-version-greater",
//			state: map[string]Entry{
//				"address1": {
//					Seq:     5,
//					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//					Version: 10,
//				},
//			},
//			newAddr: "address1",
//			newEntry: Entry{
//				Seq:     5,
//				NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
//				Version: 11,
//			},
//			updated: true,
//		},
//	}
//
//	for _, tc := range table {
//		e := tc
//		t.Run(e.name, func(t *testing.T) {
//			t.Parallel()
//
//			newSeq, updated := e.state.checkUpdated(e.newAddr, e.newEntry)
//			assert.Equal(t, e.updated, updated)
//			assert.Equal(t, e.newSeq, newSeq)
//		})
//	}
//}
//
//func TestComputeLeader(t *testing.T) {
//	table := []struct {
//		name         string
//		selfAddr     string
//		minTime      time.Time
//		lastUpdate   map[string]time.Time
//		state        State
//		expectedID   uuid.UUID
//		expectedAddr string
//	}{
//		{
//			name:     "is-self-addr",
//			selfAddr: "address-1",
//			state: map[string]Entry{
//				"address-1": {
//					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//			},
//			minTime:      mustParse("2021-06-05T10:20:00Z"),
//			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//			expectedAddr: "address-1",
//		},
//		{
//			name:     "existing-node",
//			selfAddr: "address-1",
//			state: map[string]Entry{
//				"address-1": {
//					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-2": {
//					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//			},
//			minTime: mustParse("2021-06-05T10:20:00Z"),
//			lastUpdate: map[string]time.Time{
//				"address-2": mustParse("2021-06-05T10:20:01Z"),
//			},
//			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//			expectedAddr: "address-2",
//		},
//		{
//			name:     "existing-node-out-of-sync",
//			selfAddr: "address-1",
//			state: map[string]Entry{
//				"address-1": {
//					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-2": {
//					NodeID:    uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
//					OutOfSync: true,
//				},
//			},
//			minTime: mustParse("2021-06-05T10:20:00Z"),
//			lastUpdate: map[string]time.Time{
//				"address-2": mustParse("2021-06-05T10:20:01Z"),
//			},
//			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//			expectedAddr: "address-1",
//		},
//		{
//			name:     "existing-nodes-no-last-update",
//			selfAddr: "address-1",
//			state: map[string]Entry{
//				"address-1": {
//					NodeID: uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-2": {
//					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-3": {
//					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//			},
//			minTime:      mustParse("2021-06-05T10:20:00Z"),
//			lastUpdate:   map[string]time.Time{},
//			expectedID:   uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
//			expectedAddr: "address-1",
//		},
//		{
//			name:     "existing-nodes-with-last-update-greater",
//			selfAddr: "address-1",
//			state: map[string]Entry{
//				"address-1": {
//					NodeID: uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-2": {
//					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-3": {
//					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//			},
//			minTime: mustParse("2021-06-05T10:20:00Z"),
//			lastUpdate: map[string]time.Time{
//				"address-3": mustParse("2021-06-05T10:20:01Z"),
//			},
//			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//			expectedAddr: "address-3",
//		},
//		{
//			name:     "existing-nodes-with-last-update-equals-min",
//			selfAddr: "address-1",
//			state: map[string]Entry{
//				"address-1": {
//					NodeID: uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-2": {
//					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//				"address-3": {
//					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
//				},
//			},
//			minTime: mustParse("2021-06-05T10:20:00Z"),
//			lastUpdate: map[string]time.Time{
//				"address-3": mustParse("2021-06-05T10:20:00Z"),
//				"address-2": mustParse("2021-06-05T10:20:01Z"),
//			},
//			expectedID:   uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
//			expectedAddr: "address-2",
//		},
//	}
//
//	for _, tc := range table {
//		e := tc
//		t.Run(e.name, func(t *testing.T) {
//			t.Parallel()
//
//			id, addr := e.state.computeLeader(e.selfAddr, e.minTime, e.lastUpdate)
//			assert.Equal(t, e.expectedID, id)
//			assert.Equal(t, e.expectedAddr, addr)
//		})
//	}
//}
