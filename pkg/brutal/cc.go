package brutal

import (
	"sync"
	"time"
)

type BrutalPacer struct {
	mu         sync.Mutex
	targetBps  int64
	tokens     float64
	maxTokens  float64
	lastRefill time.Time
}

func NewBrutalPacer(targetMbps float64) *BrutalPacer {
	bps := int64(targetMbps * 1024 * 1024 / 8)
	maxTok := float64(bps) * 0.1
	return &BrutalPacer{
		targetBps:  bps,
		tokens:     maxTok,
		maxTokens:  maxTok,
		lastRefill: time.Now(),
	}
}

func (p *BrutalPacer) Wait(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.refill()

	need := float64(n)
	for p.tokens < need {
		deficit := need - p.tokens
		waitDur := time.Duration(deficit / float64(p.targetBps) * float64(time.Second))
		if waitDur < time.Microsecond {
			waitDur = time.Microsecond
		}
		p.mu.Unlock()
		time.Sleep(waitDur)
		p.mu.Lock()
		p.refill()
	}
	p.tokens -= need
	if p.tokens > p.maxTokens {
		p.tokens = p.maxTokens
	}
}

func (p *BrutalPacer) refill() {
	now := time.Now()
	elapsed := now.Sub(p.lastRefill).Seconds()
	p.tokens += elapsed * float64(p.targetBps)
	p.lastRefill = now
}

func (p *BrutalPacer) Stats() (targetBps int64, currentTokens float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.targetBps, p.tokens
}
