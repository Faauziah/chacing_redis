package cache

import (
	"chacheredis/db"
	"context"
	"time"
)

// WriteThrough implements the Write-Through caching pattern.
// In this pattern, the caching layer acts as the entry point for write operations.
// The cache synchronously updates the database and then updates itself before returning.
type WriteThrough struct {
	cache Cache
	db    *db.DB
	ttl   time.Duration
}

func NewWriteThrough(c Cache, d *db.DB, ttl time.Duration) *WriteThrough {
	return &WriteThrough{
		cache: c,
		db:    d,
		ttl:   ttl,
	}
}

// GetMahasiswa reads data with validation (Read-Through with validation)
func (wt *WriteThrough) GetMahasiswa(ctx context.Context, nim string) (db.Mahasiswa, error) {
	// 1. Validate cache entry first
	if wt.cache.Validate(ctx, nim) {
		// 2. Try to fetch from validated cache
		if cachedVal, found := wt.cache.Get(ctx, nim); found {
			var m db.Mahasiswa
			if err := UnmarshalValue(cachedVal, &m); err == nil {
				return m, nil
			}
		}
	}

	// 3. Cache miss or invalid - fetch from database
	m, err := wt.db.GetByNim(ctx, nim)
	if err != nil {
		return db.Mahasiswa{}, err
	}

	// 4. Update cache with fresh data
	wt.cache.Set(ctx, nim, m, wt.ttl)
	return m, nil
}

// SaveMahasiswa writes the data. It writes to the DB first, updates the cache,
// and only then returns back to the application.
func (wt *WriteThrough) SaveMahasiswa(ctx context.Context, m db.Mahasiswa) error {
	// 1. Synchronously write to the database
	err := wt.db.Save(ctx, m)
	if err != nil {
		return err
	}

	// 2. Synchronously write to the cache (avoiding stale reads immediately)
	wt.cache.Set(ctx, m.Nim, m, wt.ttl)

	return nil
}
