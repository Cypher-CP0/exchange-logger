package collector

import (
	"sync"
	"time"
)

// tokenBucket spreads a source's requests out to stay under its rate limit.
// Reserve takes a token now and says how long to wait before using it; the
// caller cancels the reservation if that wait doesn't fit in the round.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate float64, burst int, now time.Time) *tokenBucket {
	if burst < 1 {
		burst = 1
	}
	return &tokenBucket{rate: rate, burst: float64(burst), tokens: float64(burst), last: now}
}

func (b *tokenBucket) refill(now time.Time) {
	if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
}

// Reserve takes one token and returns the wait before it may be used.
func (b *tokenBucket) Reserve(now time.Time) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(now)
	b.tokens--
	if b.tokens >= 0 {
		return 0
	}
	if b.rate <= 0 {
		return time.Duration(1<<62 - 1)
	}
	return time.Duration(-b.tokens / b.rate * float64(time.Second))
}

// Cancel returns a token taken by Reserve that won't be used.
func (b *tokenBucket) Cancel() {
	b.mu.Lock()
	b.tokens++
	b.mu.Unlock()
}

// sourceHealth tracks 429 backoff and the failed-rounds circuit breaker for
// one source.
type sourceHealth struct {
	mu           sync.Mutex
	backoffUntil time.Time
	backoffN     int
	failedRounds int
	pausedUntil  time.Time
}

const (
	backoffBase = 5 * time.Second
	backoffMax  = 5 * time.Minute
)

// onRateLimited starts or extends an exponential backoff after an HTTP 429.
func (h *sourceHealth) onRateLimited(now time.Time, retryAfter time.Duration) time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	d := backoffBase << h.backoffN
	if d > backoffMax || d <= 0 {
		d = backoffMax
	}
	if retryAfter > d {
		d = retryAfter
	}
	if h.backoffN < 16 {
		h.backoffN++
	}
	until := now.Add(d)
	if until.After(h.backoffUntil) {
		h.backoffUntil = until
	}
	return h.backoffUntil
}

func (h *sourceHealth) onSuccess() {
	h.mu.Lock()
	h.backoffN = 0
	h.mu.Unlock()
}

func (h *sourceHealth) inBackoff(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return now.Before(h.backoffUntil)
}

func (h *sourceHealth) paused(now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return now.Before(h.pausedUntil)
}

// endRound updates the circuit breaker. A round counts as failed when the
// source was asked at least once and every attempt failed. After pauseAfter
// failed rounds in a row the source is paused for pauseFor. Returns the new
// pause deadline when the circuit just opened.
func (h *sourceHealth) endRound(now time.Time, attempted, succeeded, pauseAfter int, pauseFor time.Duration) (opened bool, until time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if attempted == 0 {
		return false, time.Time{}
	}
	if succeeded > 0 {
		h.failedRounds = 0
		return false, time.Time{}
	}
	h.failedRounds++
	if pauseAfter > 0 && h.failedRounds >= pauseAfter {
		h.failedRounds = 0
		h.pausedUntil = now.Add(pauseFor)
		return true, h.pausedUntil
	}
	return false, time.Time{}
}
