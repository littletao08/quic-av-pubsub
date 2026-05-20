package brutal

import (
	"testing"
	"time"
)

func TestPacerBasic(t *testing.T) {
	p := NewBrutalPacer(10)
	start := time.Now()
	p.Wait(1024 * 1024)
	elapsed := time.Since(start)
	t.Logf("elapsed=%v (expected ~0.8s)", elapsed)
}

func TestPacerNoWaitForSmall(t *testing.T) {
	p := NewBrutalPacer(100)
	start := time.Now()
	p.Wait(1)
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("expected no wait for 1 byte with 100Mbps pacer, took %v", elapsed)
	}
}

func BenchmarkPacerWait(b *testing.B) {
	p := NewBrutalPacer(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Wait(15000) // ~15KB per call
	}
}
