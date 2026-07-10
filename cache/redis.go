package cache

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	client          *redis.Client
	hitCounter      int
	missCounter     int
	validationStats ValidationStats
	mu              sync.RWMutex
}

func NewRedisCache(addr string) *RedisCache {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &RedisCache{
		client: rdb,
	}
}

func (r *RedisCache) Get(ctx context.Context, key string) (interface{}, bool) {
	val, err := r.client.Get(ctx, key).Result()
	r.mu.Lock()
	defer r.mu.Unlock()

	if err == redis.Nil {
		r.missCounter++
		return nil, false
	} else if err != nil {
		r.missCounter++
		return nil, false
	}

	r.hitCounter++
	return val, true
}

func (r *RedisCache) Set(ctx context.Context, key string, val interface{}, ttl time.Duration) {
	var data interface{}
	switch v := val.(type) {
	case string:
		data = v
	case []byte:
		data = v
	default:
		marshaled, err := json.Marshal(val)
		if err == nil {
			data = string(marshaled)
		} else {
			data = val
		}
	}
	r.client.Set(ctx, key, data, ttl)
}

func (r *RedisCache) Delete(ctx context.Context, key string) {
	r.client.Del(ctx, key)
}

func (r *RedisCache) ResetCounters() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hitCounter = 0
	r.missCounter = 0
	r.validationStats = ValidationStats{}
}

func (r *RedisCache) GetStats() (hits, misses int) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hitCounter, r.missCounter
}

func (r *RedisCache) GetValidationStats() ValidationStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.validationStats
}

func (r *RedisCache) Clear() {
	r.client.FlushDB(context.Background())
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hitCounter = 0
	r.missCounter = 0
	r.validationStats = ValidationStats{}
}

// Validate checks if a cache entry exists and is not expired
func (r *RedisCache) Validate(ctx context.Context, key string) bool {
	r.mu.Lock()
	r.validationStats.Validations++
	r.mu.Unlock()

	ttl := r.client.TTL(ctx, key).Val()

	if ttl < 0 {
		r.mu.Lock()
		r.validationStats.Invalidations++
		r.mu.Unlock()
		return false
	}

	return true
}

// Evict removes expired keys (Redis handles this automatically, but we can trigger cleanup)
func (r *RedisCache) Evict(policy EvictionPolicy) int {
	// Redis handles eviction automatically based on maxmemory-policy
	// We can scan and delete expired keys manually
	ctx := context.Background()
	keys, _ := r.client.Keys(ctx, "*").Result()

	evicted := 0
	for _, key := range keys {
		ttl := r.client.TTL(ctx, key).Val()
		if ttl < 0 {
			r.client.Del(ctx, key)
			evicted++
		}
	}

	r.mu.Lock()
	r.validationStats.Evictions += evicted
	r.mu.Unlock()

	return evicted
}
