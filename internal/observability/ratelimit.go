package observability

import (
	"sync"
	"time"
)

type RateLimiter struct {
	mu         sync.Mutex
	rate       float64
	burst      float64
	maxTenants int
	buckets    map[string]bucket
}
type bucket struct {
	tokens  float64
	updated time.Time
}

func NewRateLimiter(rate float64, burst int, maxTenants int) *RateLimiter {
	if maxTenants <= 0 {
		maxTenants = 10_000
	}
	return &RateLimiter{rate: rate, burst: float64(burst), maxTenants: maxTenants, buckets: make(map[string]bucket)}
}
func (l *RateLimiter) Allow(tenant string, now time.Time) bool {
	if l == nil || l.rate <= 0 || l.burst <= 0 {
		return true
	}
	key := Hash(tenant)
	l.mu.Lock()
	defer l.mu.Unlock()
	value, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= l.maxTenants {
			return false
		}
		value = bucket{tokens: l.burst, updated: now}
	}
	elapsed := now.Sub(value.updated).Seconds()
	if elapsed > 0 {
		value.tokens = min(l.burst, value.tokens+elapsed*l.rate)
		value.updated = now
	}
	allowed := value.tokens >= 1
	if allowed {
		value.tokens--
	}
	l.buckets[key] = value
	return allowed
}
