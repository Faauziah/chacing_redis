package cache

import (
	"chacheredis/db"
	"context"
	"sync"
	"time"
)

// WriteBack implements the Write-Back (Write-Behind) pattern.
// In this pattern, the application writes to the cache, which acknowledges immediately.
// A background worker writes updates to the database asynchronously.
type WriteBack struct {
	cache      Cache
	db         *db.DB
	ttl        time.Duration
	writeQueue chan db.Mahasiswa
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

func NewWriteBack(c Cache, d *db.DB, ttl time.Duration, queueSize int) *WriteBack {
	ctx, cancel := context.WithCancel(context.Background())
	wb := &WriteBack{
		cache:      c,
		db:         d,
		ttl:        ttl,
		writeQueue: make(chan db.Mahasiswa, queueSize),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Start background worker goroutine
	wb.wg.Add(1)
	go wb.worker()

	return wb
}

func (wb *WriteBack) worker() {
	defer wb.wg.Done()
	for {
		select {
		case m, ok := <-wb.writeQueue:
			if !ok {
				return
			}
			// Write to the DB asynchronously in the background
			_ = wb.db.Save(wb.ctx, m)
		case <-wb.ctx.Done():
			// Process any remaining items in the queue before stopping
			// Use context.Background() since wb.ctx is cancelled
			for m := range wb.writeQueue {
				_ = wb.db.Save(context.Background(), m)
			}
			return
		}
	}
}

// GetMahasiswa reads from cache or DB with validation (Read-Through with validation)
func (wb *WriteBack) GetMahasiswa(ctx context.Context, nim string) (db.Mahasiswa, error) {
	// 1. Validate cache entry first
	if wb.cache.Validate(ctx, nim) {
		// 2. Try to fetch from validated cache
		if cachedVal, found := wb.cache.Get(ctx, nim); found {
			var m db.Mahasiswa
			if err := UnmarshalValue(cachedVal, &m); err == nil {
				return m, nil
			}
		}
	}

	// 3. Cache miss or invalid - fetch from database
	m, err := wb.db.GetByNim(ctx, nim)
	if err != nil {
		return db.Mahasiswa{}, err
	}

	// 4. Update cache with fresh data
	wb.cache.Set(ctx, nim, m, wb.ttl)
	return m, nil
}

// SaveMahasiswa writes to the cache immediately and queues the DB update in the background.
func (wb *WriteBack) SaveMahasiswa(ctx context.Context, m db.Mahasiswa) error {
	// 1. Update cache synchronously (immediate feedback to client)
	wb.cache.Set(ctx, m.Nim, m, wb.ttl)

	// 2. Queue for background database persistence (asynchronous)
	select {
	case wb.writeQueue <- m:
		// Queued successfully
	default:
		// Queue is full: block or handle queue full error. For demo, we block.
		wb.writeQueue <- m
	}

	return nil
}

// Close gracefully flushes the write queue and terminates the background worker.
func (wb *WriteBack) Close() {
	wb.cancel()
	close(wb.writeQueue)
	wb.wg.Wait()
}

// GetQueueLength returns the number of items currently waiting in the write queue
func (wb *WriteBack) GetQueueLength() int {
	return len(wb.writeQueue)
}

// Flush forces all queued writes to be processed immediately
func (wb *WriteBack) Flush(ctx context.Context) error {
	// Get current queue length
	queueLen := len(wb.writeQueue)

	// Process all items currently in queue
	for i := 0; i < queueLen; i++ {
		select {
		case m := <-wb.writeQueue:
			if err := wb.db.Save(ctx, m); err != nil {
				// Re-queue on error
				wb.writeQueue <- m
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	return nil
}
