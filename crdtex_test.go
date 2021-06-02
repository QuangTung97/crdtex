package crdtex

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

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
					Seq:     1,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
				"address2": {
					Seq:     1,
					NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
					Version: 2,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     1,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
				"address2": {
					Seq:     1,
					NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
					Version: 2,
				},
			},
		},
		{
			name: "b-non-empty",
			b: map[string]Entry{
				"address1": {
					Seq:     1,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
				"address2": {
					Seq:     1,
					NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
					Version: 2,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     1,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
				"address2": {
					Seq:     1,
					NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
					Version: 2,
				},
			},
		},
		{
			name: "a-b-different-keys",
			a: map[string]Entry{
				"address1": {
					Seq:     1,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
			},
			b: map[string]Entry{
				"address2": {
					Seq:     1,
					NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
					Version: 2,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     1,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
				"address2": {
					Seq:     1,
					NodeID:  uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"),
					Version: 2,
				},
			},
		},
		{
			name: "a-b-same-key-a-higher-seq",
			a: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:     1,
					NodeID:  uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
		},
		{
			name: "a-b-same-key-b-higher-seq",
			a: map[string]Entry{
				"address1": {
					Seq:     1,
					NodeID:  uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 1,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
		},
		{
			name: "a-b-same-key-same-seq-a-id-less",
			a: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("a1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 11,
				},
			},
		},
		{
			name: "a-b-same-key-same-seq-b-id-less",
			a: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
		},
		{
			name: "a-b-same-key-same-seq-same-id-a-version-less",
			a: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 11,
				},
			},
		},
		{
			name: "a-b-same-key-same-seq-same-id-a-version-greater",
			a: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 12,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 11,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:     2,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 12,
				},
			},
		},
		{
			name: "a-b-same-key-same-seq-same-id-same-version-a-out-of-sync-true",
			a: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: true,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: false,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: true,
				},
			},
		},
		{
			name: "a-b-same-key-same-seq-same-id-same-version-b-out-of-sync-true",
			a: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: false,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: true,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: true,
				},
			},
		},
		{
			name: "a-b-same-key-same-seq-same-id-same-version-out-of-sync-both-true",
			a: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: true,
				},
			},
			b: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version:   12,
					OutOfSync: true,
				},
			},
			expected: map[string]Entry{
				"address1": {
					Seq:       2,
					NodeID:    uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
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
		newSeq   uint64
		updated  bool
	}{
		{
			name:    "entry-not-existed",
			newAddr: "address1",
			newEntry: Entry{
				Seq:     1,
				NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
				Version: 10,
			},
			updated: true,
		},
		{
			name: "entry-existed-different-id-seq-less",
			state: map[string]Entry{
				"address1": {
					Seq:     5,
					NodeID:  uuid.MustParse("cda641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Seq:     4,
				NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
				Version: 20,
			},
			newSeq:  6,
			updated: false,
		},
		{
			name: "entry-existed-same-seq-id-less",
			state: map[string]Entry{
				"address1": {
					Seq:     5,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Seq:     5,
				NodeID:  uuid.MustParse("a1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
				Version: 20,
			},
			newSeq:  6,
			updated: false,
		},
		{
			name: "entry-existed-seq-less-id-greater",
			state: map[string]Entry{
				"address1": {
					Seq:     5,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Seq:     4,
				NodeID:  uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
				Version: 20,
			},
			newSeq:  5,
			updated: false,
		},
		{
			name: "entry-existed-same-id-same-seq-version-greater",
			state: map[string]Entry{
				"address1": {
					Seq:     5,
					NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
					Version: 10,
				},
			},
			newAddr: "address1",
			newEntry: Entry{
				Seq:     5,
				NodeID:  uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
				Version: 11,
			},
			updated: true,
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			newSeq, updated := e.state.checkUpdated(e.newAddr, e.newEntry)
			assert.Equal(t, e.updated, updated)
			assert.Equal(t, e.newSeq, newSeq)
		})
	}
}

func TestUUIDLess(t *testing.T) {
	table := []struct {
		name     string
		a        uuid.UUID
		b        uuid.UUID
		expected bool
	}{
		{
			name:     "equal",
			a:        uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			b:        uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			expected: false,
		},
		{
			name:     "less",
			a:        uuid.MustParse("a1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			b:        uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			expected: true,
		},
		{
			name:     "less",
			a:        uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			b:        uuid.MustParse("b2a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			expected: true,
		},
		{
			name:     "greater",
			a:        uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			b:        uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			expected: false,
		},
		{
			name:     "greater",
			a:        uuid.MustParse("c1a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			b:        uuid.MustParse("b2a641f5-0770-4ef7-9d58-0d6b3e75a355"),
			expected: false,
		},
	}
	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := uuidLess(e.a, e.b)
			assert.Equal(t, e.expected, result)
		})
	}
}

func TestComputeLeader(t *testing.T) {
	table := []struct {
		name         string
		selfAddr     string
		minTime      time.Time
		lastUpdate   map[string]time.Time
		state        State
		expectedID   uuid.UUID
		expectedAddr string
	}{
		{
			name:     "is-self-addr",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
				},
			},
			minTime:      mustParse("2021-06-05T10:20:00Z"),
			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
			expectedAddr: "address-1",
		},
		{
			name:     "existing-node",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-2": {
					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-2": mustParse("2021-06-05T10:20:01Z"),
			},
			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
			expectedAddr: "address-2",
		},
		{
			name:     "existing-node-out-of-sync",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-2": {
					NodeID:    uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
					OutOfSync: true,
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-2": mustParse("2021-06-05T10:20:01Z"),
			},
			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
			expectedAddr: "address-1",
		},
		{
			name:     "existing-nodes-no-last-update",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					NodeID: uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-2": {
					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-3": {
					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
				},
			},
			minTime:      mustParse("2021-06-05T10:20:00Z"),
			lastUpdate:   map[string]time.Time{},
			expectedID:   uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
			expectedAddr: "address-1",
		},
		{
			name:     "existing-nodes-with-last-update-greater",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					NodeID: uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-2": {
					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-3": {
					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-3": mustParse("2021-06-05T10:20:01Z"),
			},
			expectedID:   uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
			expectedAddr: "address-3",
		},
		{
			name:     "existing-nodes-with-last-update-equals-min",
			selfAddr: "address-1",
			state: map[string]Entry{
				"address-1": {
					NodeID: uuid.MustParse("c93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-2": {
					NodeID: uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
				},
				"address-3": {
					NodeID: uuid.MustParse("a93cb116-6b95-4d8e-893f-86106185b638"),
				},
			},
			minTime: mustParse("2021-06-05T10:20:00Z"),
			lastUpdate: map[string]time.Time{
				"address-3": mustParse("2021-06-05T10:20:00Z"),
				"address-2": mustParse("2021-06-05T10:20:01Z"),
			},
			expectedID:   uuid.MustParse("b93cb116-6b95-4d8e-893f-86106185b638"),
			expectedAddr: "address-2",
		},
	}

	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			id, addr := e.state.computeLeader(e.selfAddr, e.minTime, e.lastUpdate)
			assert.Equal(t, e.expectedID, id)
			assert.Equal(t, e.expectedAddr, addr)
		})
	}
}
