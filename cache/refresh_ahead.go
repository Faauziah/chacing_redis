package cache

import (
	"context"
	"sync"
	"time"
	"chacheredis/db"
)

type RefreshItem struct {
	Data     db.Mahasiswa
	CachedAt time.Time
	TTL      time.Duration
}

// RefreshAhead implements the Refresh-Ahead caching pattern.
// If a cached item is requested and is close to expiration (e.g., remaining TTL <= 30%),
// the cache returns the current cached value immediately, but triggers an asynchronous
// background worker to refresh the value from the database.
type RefreshAhead struct {
	cache      Cache
	db         *db.DB
	ttl        time.Duration
	threshold  float64  // Percentage of TTL remaining to trigger refresh (e.g. 0.3)
	refreshing sync.Map // Prevents duplicate concurrent background refreshes for the same key
}

func NewRefreshAhead(c Cache, d *db.DB, ttl time.Duration, threshold float64) *RefreshAhead {
	return &RefreshAhead{
		cache:     c,
		db:        d,
		ttl:       ttl,
		threshold: threshold,
	}
}

// GetMahasiswa retrieves a student. If the item's remaining life is low,
// it triggers a background reload while returning the current cached copy immediately.
func (ra *RefreshAhead) GetMahasiswa(ctx context.Context, nim string) (db.Mahasiswa, error) {
	val, found := ra.cache.Get(ctx, nim)
	if found {
		var item RefreshItem
		if err := UnmarshalValue(val, &item); err == nil {
			elapsed := time.Since(item.CachedAt)
			remaining := item.TTL - elapsed

			// If remaining TTL is less than or equal to the threshold (e.g. 30% of TTL),
			// trigger an asynchronous refresh
			thresholdDuration := time.Duration(float64(item.TTL) * ra.threshold)
			if remaining <= thresholdDuration {
				ra.triggerBackgroundRefresh(nim)
			}

			return item.Data, nil
		}
	}

	// Cache Miss: Fetch synchronously from database
	m, err := ra.db.GetByNim(ctx, nim)
	if err != nil {
		return db.Mahasiswa{}, err
	}

	// Store wrapped item with caching timestamps
	ra.cache.Set(ctx, nim, RefreshItem{
		Data:     m,
		CachedAt: time.Now(),
		TTL:      ra.ttl,
	}, ra.ttl)

	return m, nil
}

// SaveMahasiswa writes to the DB and invalidates cache (standard practice for writes in refresh-ahead)
func (ra *RefreshAhead) SaveMahasiswa(ctx context.Context, m db.Mahasiswa) error {
	err := ra.db.Save(ctx, m)
	if err != nil {
		return err
	}
	ra.cache.Delete(ctx, m.Nim)
	return nil
}

// triggerBackgroundRefresh starts a goroutine to load data from DB and update the cache
func (ra *RefreshAhead) triggerBackgroundRefresh(nim string) {
	// Ensure we only run one refresh at a time for this key
	if _, loading := ra.refreshing.LoadOrStore(nim, true); loading {
		return
	}

	go func() {
		defer ra.refreshing.Delete(nim)

		// Fetch the latest data from the database
		m, err := ra.db.GetByNim(context.Background(), nim)
		if err != nil {
			return
		}

		// Update the cache with refreshed data
		ra.cache.Set(context.Background(), nim, RefreshItem{
			Data:     m,
			CachedAt: time.Now(),
			TTL:      ra.ttl,
		}, ra.ttl)
	}()
}
