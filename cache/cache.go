package cache

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type CacheItem struct {
	Value        interface{}
	Expiration   time.Time
	LastAccessed time.Time
	AccessCount  int
	CreatedAt    time.Time
}

func (item CacheItem) Expired() bool {
	if item.Expiration.IsZero() {
		return false
	}
	return time.Now().After(item.Expiration)
}

func (item CacheItem) IsValid() bool {
	return !item.Expired()
}

// EvictionPolicy defines cache eviction strategies
type EvictionPolicy int

const (
	LRU  EvictionPolicy = iota // Least Recently Used
	LFU                        // Least Frequently Used
	FIFO                       // First In First Out
)

// ValidationStats tracks cache validation metrics
type ValidationStats struct {
	Validations    int
	Invalidations  int
	Evictions      int
	ExpiredEntries int
}

// Cache defines the standard interface for our caching mechanisms
type Cache interface {
	Get(ctx context.Context, key string) (interface{}, bool)
	Set(ctx context.Context, key string, val interface{}, ttl time.Duration)
	Delete(ctx context.Context, key string)
	ResetCounters()
	GetStats() (hits, misses int)
	GetValidationStats() ValidationStats
	Clear()
	Evict(policy EvictionPolicy) int
	Validate(ctx context.Context, key string) bool
}

type InMemoryCache struct {
	mu              sync.RWMutex
	items           map[string]CacheItem
	hitCounter      int
	missCounter     int
	validationStats ValidationStats
	maxSize         int
}

func NewInMemoryCache() *InMemoryCache {
	return &InMemoryCache{
		items:   make(map[string]CacheItem),
		maxSize: 1000, // Default max size
	}
}

func NewInMemoryCacheWithSize(maxSize int) *InMemoryCache {
	return &InMemoryCache{
		items:   make(map[string]CacheItem),
		maxSize: maxSize,
	}
}

func (c *InMemoryCache) Get(ctx context.Context, key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	item, exists := c.items[key]
	if !exists {
		c.missCounter++
		return nil, false
	}

	if item.Expired() {
		delete(c.items, key)
		c.missCounter++
		c.validationStats.ExpiredEntries++
		return nil, false
	}

	// Update access tracking for LRU/LFU
	item.LastAccessed = time.Now()
	item.AccessCount++
	c.items[key] = item

	c.hitCounter++
	return item.Value, true
}

func (c *InMemoryCache) Set(ctx context.Context, key string, val interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if cache is full, evict using LRU
	if len(c.items) >= c.maxSize {
		c.evictOne(LRU)
	}

	var expr time.Time
	if ttl > 0 {
		expr = time.Now().Add(ttl)
	}

	now := time.Now()
	c.items[key] = CacheItem{
		Value:        val,
		Expiration:   expr,
		LastAccessed: now,
		AccessCount:  0,
		CreatedAt:    now,
	}
}

func (c *InMemoryCache) Delete(ctx context.Context, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *InMemoryCache) ResetCounters() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hitCounter = 0
	c.missCounter = 0
	c.validationStats = ValidationStats{}
}

func (c *InMemoryCache) GetStats() (hits, misses int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hitCounter, c.missCounter
}

func (c *InMemoryCache) GetValidationStats() ValidationStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.validationStats
}

func (c *InMemoryCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items = make(map[string]CacheItem)
	c.hitCounter = 0
	c.missCounter = 0
	c.validationStats = ValidationStats{}
}

// Validate checks if a cache entry is valid (exists and not expired)
func (c *InMemoryCache) Validate(ctx context.Context, key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.validationStats.Validations++

	item, exists := c.items[key]
	if !exists {
		c.validationStats.Invalidations++
		return false
	}

	if item.Expired() {
		delete(c.items, key)
		c.validationStats.Invalidations++
		c.validationStats.ExpiredEntries++
		return false
	}

	return true
}

// Evict removes entries based on the specified policy
func (c *InMemoryCache) Evict(policy EvictionPolicy) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	evicted := 0
	toEvict := len(c.items) / 4 // Evict 25% of entries

	for i := 0; i < toEvict; i++ {
		if c.evictOne(policy) {
			evicted++
		}
	}

	return evicted
}

// evictOne removes one entry based on policy (must be called with lock held)
func (c *InMemoryCache) evictOne(policy EvictionPolicy) bool {
	if len(c.items) == 0 {
		return false
	}

	var victimKey string
	switch policy {
	case LRU:
		// Find least recently used
		var oldestTime time.Time
		for key, item := range c.items {
			if oldestTime.IsZero() || item.LastAccessed.Before(oldestTime) {
				oldestTime = item.LastAccessed
				victimKey = key
			}
		}
	case LFU:
		// Find least frequently used
		minCount := -1
		for key, item := range c.items {
			if minCount == -1 || item.AccessCount < minCount {
				minCount = item.AccessCount
				victimKey = key
			}
		}
	case FIFO:
		// Find oldest entry
		var oldestTime time.Time
		for key, item := range c.items {
			if oldestTime.IsZero() || item.CreatedAt.Before(oldestTime) {
				oldestTime = item.CreatedAt
				victimKey = key
			}
		}
	}

	if victimKey != "" {
		delete(c.items, victimKey)
		c.validationStats.Evictions++
		return true
	}

	return false
}

// UnmarshalValue handles converting cached data (string/JSON from Redis or raw interface) into the target struct.
func UnmarshalValue(val interface{}, target interface{}) error {
	if str, ok := val.(string); ok {
		return json.Unmarshal([]byte(str), target)
	}
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
