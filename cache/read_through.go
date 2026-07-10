package cache

import (
	"context"
	"time"
	"chacheredis/db"
)

type ReadThrough struct {
	cache Cache
	db    *db.DB
	ttl   time.Duration
}

func NewReadThrough(c Cache, d *db.DB, ttl time.Duration) *ReadThrough {
	return &ReadThrough{
		cache: c,
		db:    d,
		ttl:   ttl,
	}
}

func (rt *ReadThrough) GetMahasiswa(ctx context.Context, nim string) (db.Mahasiswa, error) {
	// 1. Query the cache
	if cachedVal, found := rt.cache.Get(ctx, nim); found {
		var m db.Mahasiswa
		if err := UnmarshalValue(cachedVal, &m); err == nil {
			return m, nil
		}
	}

	// 2. Cache Miss: The Read-Through library handles loading from the DB internally
	m, err := rt.db.GetByNim(ctx, nim)
	if err != nil {
		return db.Mahasiswa{}, err
	}

	// 3. The library populates itself
	rt.cache.Set(ctx, nim, m, rt.ttl)

	// 4. Return to the application
	return m, nil
}
