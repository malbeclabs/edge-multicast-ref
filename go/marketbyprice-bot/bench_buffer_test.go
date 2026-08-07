package main

import (
	"fmt"
	"testing"
)

// BenchmarkBufferDelta measures the per-append cost of bufferDelta as one
// instrument's buffer fills. Records arrive in mktdata-seq order, which is the
// live case; a per-append full sort makes this quadratic.
func BenchmarkBufferDelta(b *testing.B) {
	for _, n := range []int{1000, 10000, 40000} {
		b.Run(fmt.Sprintf("fill-%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				s := NewShard(0, 1, nil)
				k := instKey{1, 7}
				for j := 0; j < n; j++ {
					s.bufferDelta(k, Record{SequenceNumber: uint64(j)})
				}
			}
		})
	}
}
