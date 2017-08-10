package kcl

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSequencePairIsLessThan(t *testing.T) {
	assert := assert.New(t)

	big10 := big.NewInt(10)
	big5 := big.NewInt(5)

	tests := []struct {
		left   SequencePair
		right  SequencePair
		isLess bool
	}{
		{left: SequencePair{nil, 0}, right: SequencePair{nil, 0}, isLess: false},
		{left: SequencePair{nil, 0}, right: SequencePair{big10, 0}, isLess: false},
		{left: SequencePair{big10, 0}, right: SequencePair{nil, 0}, isLess: false},

		{left: SequencePair{big5, 0}, right: SequencePair{big10, 0}, isLess: true},
		{left: SequencePair{big5, 0}, right: SequencePair{big5, 10}, isLess: true},

		{left: SequencePair{big10, 0}, right: SequencePair{big5, 0}, isLess: false},
		{left: SequencePair{big5, 10}, right: SequencePair{big5, 0}, isLess: false},
	}

	for _, test := range tests {
		left := test.left
		right := test.right
		t.Logf(
			"Is <%s, %d> less than <%s, %d>? %t",
			left.Sequence.String(), left.SubSequence,
			right.Sequence.String(), right.SubSequence,
			test.isLess,
		)

		assert.Equal(test.isLess, left.IsLessThan(right))
	}
}

func TestSequencePairEmpty(t *testing.T) {
	assert := assert.New(t)

	assert.True(SequencePair{nil, 0}.IsNil())
	assert.True(SequencePair{nil, 10000}.IsNil())

	assert.False(SequencePair{big.NewInt(10), 0}.IsNil())
	assert.False(SequencePair{big.NewInt(0), 1000}.IsNil())
}
