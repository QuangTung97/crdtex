package crdtex

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
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
			expected: map[uuid.UUID]Entry{},
		},
		{
			name: "a-non-empty",
			a: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr: "address1",
				},
				uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"): {
					Addr: "address2",
				},
			},
			expected: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr: "address1",
				},
				uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"): {
					Addr: "address2",
				},
			},
		},
		{
			name: "b-non-empty",
			b: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr: "address1",
				},
				uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"): {
					Addr: "address2",
				},
			},
			expected: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr: "address1",
				},
				uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"): {
					Addr: "address2",
				},
			},
		},
		{
			name: "a-b-different-keys",
			a: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr: "address1",
				},
			},
			b: map[uuid.UUID]Entry{
				uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"): {
					Addr: "address2",
				},
			},
			expected: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr: "address1",
				},
				uuid.MustParse("535dbd7a-9a65-48b3-8644-0fb58eed98d7"): {
					Addr: "address2",
				},
			},
		},
		{
			name: "a-b-same-key-a-higher-version",
			a: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1a",
					Version: 2,
				},
			},
			b: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1",
					Version: 1,
				},
			},
			expected: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1a",
					Version: 2,
				},
			},
		},
		{
			name: "a-b-same-key-b-higher-version",
			a: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1a",
					Version: 2,
				},
			},
			b: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1b",
					Version: 3,
				},
			},
			expected: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1b",
					Version: 3,
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

func TestComputeAddressSet(t *testing.T) {
	table := []struct {
		name     string
		state    State
		expected []string
	}{
		{
			name:     "empty",
			state:    map[uuid.UUID]Entry{},
			expected: []string{},
		},
		{
			name: "single",
			state: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1",
					Version: 3,
				},
			},
			expected: []string{"address1"},
		},
		{
			name: "two-same-addr",
			state: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address1",
					Version: 3,
				},
				uuid.MustParse("08998891-6bd6-424f-a722-51b761865ef0"): {
					Addr:    "address1",
					Version: 4,
				},
			},
			expected: []string{"address1"},
		},
		{
			name: "two-not-same-addr",
			state: map[uuid.UUID]Entry{
				uuid.MustParse("b1a641f5-0770-4ef7-9d58-0d6b3e75a355"): {
					Addr:    "address2",
					Version: 3,
				},
				uuid.MustParse("08998891-6bd6-424f-a722-51b761865ef0"): {
					Addr:    "address1",
					Version: 4,
				},
			},
			expected: []string{"address1", "address2"},
		},
	}
	for _, tc := range table {
		e := tc
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := computeAddressSet(e.state)
			assert.Equal(t, e.expected, result)
		})
	}
}
