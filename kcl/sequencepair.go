package kcl

import (
	"math/big"
)

// SequencePair a convience way to pass around a Sequence / SubSequence pair
type SequencePair struct {
	Sequence    *big.Int
	SubSequence int
}

func (s SequencePair) IsNil() bool {
	return s.Sequence == nil
}

func (s SequencePair) IsLessThan(pair SequencePair) bool {
	if s.IsNil() || pair.IsNil() { // empty pairs are incomparable
		return false
	}

	cmp := s.Sequence.Cmp(pair.Sequence)
	if cmp == -1 {
		return true
	}
	if cmp == 1 {
		return false
	}

	return s.SubSequence < pair.SubSequence
}
