package cache

import (
	"chacheredis/db"
	"context"
	"time"
)

type CacheAside struct {
	cache Cache
	db    *db.DB
	ttl   time.Duration
}

func NewCacheAside(c Cache, d *db.DB, ttl time.Duration) *CacheAside {
	return &CacheAside{
		cache: c,
		db:    d,
		ttl:   ttl,
	}
}

func (ca *CacheAside) GetMahasiswa(ctx context.Context, nim string) (db.Mahasiswa, error) {
	if ca.cache.Validate(ctx, nim) {
		if cachedVal, found := ca.cache.Get(ctx, nim); found {
			var m db.Mahasiswa
			if err := UnmarshalValue(cachedVal, &m); err == nil {
				return m, nil
			}
		}
	}

	m, err := ca.db.GetByNim(ctx, nim)
	if err != nil {
		return db.Mahasiswa{}, err
	}

	ca.cache.Set(ctx, nim, m, ca.ttl)
	return m, nil
}

func (ca *CacheAside) SaveMahasiswa(ctx context.Context, m db.Mahasiswa) error {
	err := ca.db.Save(ctx, m)
	if err != nil {
		return err
	}
	ca.cache.Delete(ctx, m.Nim)
	return nil
}
