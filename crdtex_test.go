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
