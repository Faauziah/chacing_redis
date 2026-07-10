package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"chacheredis/cache"
	"chacheredis/db"
)

type Response struct {
	Data    interface{} `json:"data,omitempty"`
	Message string      `json:"message,omitempty"`
	Latency string      `json:"latency"`
	Stats   Stats       `json:"stats"`
}

type Stats struct {
	Strategy        string                 `json:"strategy"`
	DBHits          int                    `json:"db_hits"`
	CacheHits       int                    `json:"cache_hits"`
	CacheMisses     int                    `json:"cache_misses"`
	ValidationStats *cache.ValidationStats `json:"validation_stats,omitempty"`
	QueueLength     *int                   `json:"queue_length,omitempty"`
}

func main() {
	ctx := context.Background()

	mysqlDSN := "root:@tcp(localhost:3306)/chacheredis?parseTime=true&charset=utf8mb4"
	database, err := db.NewDB(mysqlDSN)
	if err != nil {
		fmt.Printf("Error koneksi MySQL: %v\n", err)
		return
	}
	defer database.Close()

	fallbackData := []db.Mahasiswa{
		{NamaKelas: "6RPL-A", Nim: "105841104423", Nama: "nurul", Email: "nurulmustainna@gmail.com", EmailStudent: "105841104423@student.unismuh.ac.id"},
		{NamaKelas: "6RPL-B", Nim: "105841104223", Nama: "fauziah", Email: "fauziahonlly@gmail.com", EmailStudent: "105841104223@student.unismuh.ac.id"},
	}
	for _, s := range fallbackData {
		_ = database.Save(ctx, s)
	}

	defaultNim := "105841104423"
	redisAddr := "localhost:6379"

	redisCacheAside := cache.NewRedisCache(redisAddr)
	ca := cache.NewCacheAside(redisCacheAside, database, 60*time.Second)

	redisCacheReadThrough := cache.NewRedisCache(redisAddr)
	rt := cache.NewReadThrough(redisCacheReadThrough, database, 60*time.Second)

	redisCacheWriteThrough := cache.NewRedisCache(redisAddr)
	wt := cache.NewWriteThrough(redisCacheWriteThrough, database, 60*time.Second)

	redisCacheWriteBack := cache.NewRedisCache(redisAddr)
	wb := cache.NewWriteBack(redisCacheWriteBack, database, 60*time.Second, 100)
	defer wb.Close()

	redisCacheRefreshAhead := cache.NewRedisCache(redisAddr)
	ra := cache.NewRefreshAhead(redisCacheRefreshAhead, database, 20*time.Second, 0.5)

	http.HandleFunc("/api/cache-aside", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			nim := r.URL.Query().Get("nim")
			if nim == "" {
				nim = defaultNim
			}
			start := time.Now()
			m, err := ca.GetMahasiswa(ctx, nim)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			hits, misses := redisCacheAside.GetStats()
			validationStats := redisCacheAside.GetValidationStats()
			json.NewEncoder(w).Encode(Response{
				Data:    m,
				Latency: dur.String(),
				Stats: Stats{
					Strategy:        "Cache-Aside (Redis)",
					DBHits:          database.GetQueryCount(),
					CacheHits:       hits,
					CacheMisses:     misses,
					ValidationStats: &validationStats,
				},
			})
		} else if r.Method == http.MethodPost {
			var m db.Mahasiswa
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON"})
				return
			}

			start := time.Now()
			err := ca.SaveMahasiswa(ctx, m)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			json.NewEncoder(w).Encode(map[string]interface{}{
				"message": "Data saved successfully (Cache-Aside: DB updated, cache invalidated)",
				"latency": dur.String(),
				"data":    m,
			})
		}
	})

	// 2. ENDPOINT: READ-THROUGH (GET untuk Baca)
	http.HandleFunc("/api/read-through", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			nim := r.URL.Query().Get("nim")
			if nim == "" {
				nim = defaultNim
			}
			start := time.Now()
			m, err := rt.GetMahasiswa(ctx, nim)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}
			hits, misses := redisCacheReadThrough.GetStats()
			validationStats := redisCacheReadThrough.GetValidationStats()
			json.NewEncoder(w).Encode(Response{
				Data:    m,
				Latency: dur.String(),
				Stats: Stats{
					Strategy:        "Read-Through (Redis)",
					DBHits:          database.GetQueryCount(),
					CacheHits:       hits,
					CacheMisses:     misses,
					ValidationStats: &validationStats,
				},
			})
		}
	})

	// 3. ENDPOINT: WRITE-THROUGH (GET untuk Baca, POST untuk Update)
	http.HandleFunc("/api/write-through", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			nim := r.URL.Query().Get("nim")
			if nim == "" {
				nim = defaultNim
			}
			start := time.Now()
			m, err := wt.GetMahasiswa(ctx, nim)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			hits, misses := redisCacheWriteThrough.GetStats()
			validationStats := redisCacheWriteThrough.GetValidationStats()
			json.NewEncoder(w).Encode(Response{
				Data:    m,
				Latency: dur.String(),
				Stats: Stats{
					Strategy:        "Write-Through (Redis)",
					DBHits:          database.GetQueryCount(),
					CacheHits:       hits,
					CacheMisses:     misses,
					ValidationStats: &validationStats,
				},
			})
		} else if r.Method == http.MethodPost {
			var m db.Mahasiswa
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON"})
				return
			}

			start := time.Now()
			err := wt.SaveMahasiswa(ctx, m)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			json.NewEncoder(w).Encode(map[string]interface{}{
				"message": "Data saved successfully (Write-Through: DB and cache updated synchronously)",
				"latency": dur.String(),
				"data":    m,
			})
		}
	})

	// 4. ENDPOINT: WRITE-BACK (GET untuk Baca, POST untuk Update)
	http.HandleFunc("/api/write-back", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method == http.MethodGet {
			nim := r.URL.Query().Get("nim")
			if nim == "" {
				nim = defaultNim
			}
			start := time.Now()
			m, err := wb.GetMahasiswa(ctx, nim)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			hits, misses := redisCacheWriteBack.GetStats()
			validationStats := redisCacheWriteBack.GetValidationStats()
			queueLen := wb.GetQueueLength()
			json.NewEncoder(w).Encode(Response{
				Data:    m,
				Latency: dur.String(),
				Stats: Stats{
					Strategy:        "Write-Back (Redis)",
					DBHits:          database.GetQueryCount(),
					CacheHits:       hits,
					CacheMisses:     misses,
					ValidationStats: &validationStats,
					QueueLength:     &queueLen,
				},
			})
		} else if r.Method == http.MethodPost {
			var m db.Mahasiswa
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "Invalid JSON"})
				return
			}

			start := time.Now()
			err := wb.SaveMahasiswa(ctx, m)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			queueLen := wb.GetQueueLength()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"message":      "Data saved successfully (Write-Back: Cache updated immediately, DB queued)",
				"latency":      dur.String(),
				"data":         m,
				"queue_length": queueLen,
			})
		}
	})

	// 5. ENDPOINT: WRITE-BACK FLUSH (POST untuk memaksa flush queue)
	http.HandleFunc("/api/write-back/flush", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			start := time.Now()
			err := wb.Flush(ctx)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			json.NewEncoder(w).Encode(map[string]interface{}{
				"message": "Write-back queue flushed successfully",
				"latency": dur.String(),
			})
		}
	})

	// 6. ENDPOINT: REFRESH-AHEAD (GET untuk Baca)
	http.HandleFunc("/api/refresh-ahead", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			nim := r.URL.Query().Get("nim")
			if nim == "" {
				nim = defaultNim
			}
			start := time.Now()
			m, err := ra.GetMahasiswa(ctx, nim)
			dur := time.Since(start)

			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
				return
			}

			// Mengembalikan info TTL tambahan
			var remainingStr string = "not found/expired"
			val, found := redisCacheRefreshAhead.Get(ctx, nim)
			if found {
				var item cache.RefreshItem
				if err := cache.UnmarshalValue(val, &item); err == nil {
					elapsed := time.Since(item.CachedAt)
					remaining := item.TTL - elapsed
					remainingStr = remaining.String()
				}
			}

			hits, misses := redisCacheRefreshAhead.GetStats()
			validationStats := redisCacheRefreshAhead.GetValidationStats()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data":          m,
				"latency":       dur.String(),
				"remaining_ttl": remainingStr,
				"stats": Stats{
					Strategy:        "Refresh-Ahead (Redis)",
					DBHits:          database.GetQueryCount(),
					CacheHits:       hits,
					CacheMisses:     misses,
					ValidationStats: &validationStats,
				},
			})
		}
	})

	// 7. ENDPOINT: LARGE-DATA (Mengambil seluruh dataset dari Redis)
	// Hit 1 -> Lambat (Ambil dari API GraphQL + simpan ke Redis)
	// Hit 2 -> Cepat (Langsung baca dari Redis)
	http.HandleFunc("/api/large-data", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			start := time.Now()

			// 1. Cek apakah seluruh data besar tersimpan di Redis
			val, found := redisCacheReadThrough.Get(ctx, "large_students_data")
			if found {
				var cachedStudents []db.Mahasiswa
				if err := cache.UnmarshalValue(val, &cachedStudents); err == nil {
					dur := time.Since(start)
					hits, misses := redisCacheReadThrough.GetStats()
					json.NewEncoder(w).Encode(map[string]interface{}{
						"message": "Data besar berhasil diambil dari REDIS CONTAINER (Cache Hit)",
						"latency": dur.String(),
						"count":   len(cachedStudents),
						"stats": Stats{
							Strategy:    "Redis-Large-Data",
							DBHits:      database.GetQueryCount(),
							CacheHits:   hits,
							CacheMisses: misses,
						},
						"data": cachedStudents,
					})
					return
				}
			}

			time.Sleep(500 * time.Millisecond)
			freshStudents := database.GetAllLocal()

			redisCacheReadThrough.Set(ctx, "large_students_data", freshStudents, 120*time.Second)

			dur := time.Since(start)
			hits, misses := redisCacheReadThrough.GetStats()
			json.NewEncoder(w).Encode(map[string]interface{}{
				"message": "Data besar diambil dari DATABASE (Cache Miss)",
				"latency": dur.String(),
				"count":   len(freshStudents),
				"stats": Stats{
					Strategy:    "Redis-Large-Data",
					DBHits:      database.GetQueryCount(),
					CacheHits:   hits,
					CacheMisses: misses,
				},
				"data": freshStudents,
			})
		}
	})

	// 8. ENDPOINT: CACHE EVICTION (POST untuk trigger eviction)
	http.HandleFunc("/api/cache/evict", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			policy := r.URL.Query().Get("policy")
			var evictionPolicy cache.EvictionPolicy

			switch policy {
			case "lru":
				evictionPolicy = cache.LRU
			case "lfu":
				evictionPolicy = cache.LFU
			case "fifo":
				evictionPolicy = cache.FIFO
			default:
				evictionPolicy = cache.LRU
			}

			start := time.Now()
			evictedAside := redisCacheAside.Evict(evictionPolicy)
			evictedRT := redisCacheReadThrough.Evict(evictionPolicy)
			evictedWT := redisCacheWriteThrough.Evict(evictionPolicy)
			evictedWB := redisCacheWriteBack.Evict(evictionPolicy)
			evictedRA := redisCacheRefreshAhead.Evict(evictionPolicy)
			dur := time.Since(start)

			totalEvicted := evictedAside + evictedRT + evictedWT + evictedWB + evictedRA

			json.NewEncoder(w).Encode(map[string]interface{}{
				"message": fmt.Sprintf("Cache eviction completed using %s policy", policy),
				"latency": dur.String(),
				"evicted": map[string]int{
					"cache_aside":   evictedAside,
					"read_through":  evictedRT,
					"write_through": evictedWT,
					"write_back":    evictedWB,
					"refresh_ahead": evictedRA,
					"total":         totalEvicted,
				},
			})
		}
	})

	// 9. ENDPOINT: CACHE VALIDATION (GET untuk validasi key tertentu)
	http.HandleFunc("/api/cache/validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			nim := r.URL.Query().Get("nim")
			if nim == "" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "nim parameter is required"})
				return
			}

			validAside := redisCacheAside.Validate(ctx, nim)
			validRT := redisCacheReadThrough.Validate(ctx, nim)
			validWT := redisCacheWriteThrough.Validate(ctx, nim)
			validWB := redisCacheWriteBack.Validate(ctx, nim)
			validRA := redisCacheRefreshAhead.Validate(ctx, nim)

			json.NewEncoder(w).Encode(map[string]interface{}{
				"nim": nim,
				"validation_results": map[string]bool{
					"cache_aside":   validAside,
					"read_through":  validRT,
					"write_through": validWT,
					"write_back":    validWB,
					"refresh_ahead": validRA,
				},
				"validation_stats": map[string]cache.ValidationStats{
					"cache_aside":   redisCacheAside.GetValidationStats(),
					"read_through":  redisCacheReadThrough.GetValidationStats(),
					"write_through": redisCacheWriteThrough.GetValidationStats(),
					"write_back":    redisCacheWriteBack.GetValidationStats(),
					"refresh_ahead": redisCacheRefreshAhead.GetValidationStats(),
				},
			})
		}
	})

	// 10. ENDPOINT: RESET STATS DAN CACHE (Termasuk membersihkan data di Redis container)
	http.HandleFunc("/api/reset", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		database.ResetCounter()
		redisCacheAside.Clear()
		redisCacheReadThrough.Clear()
		redisCacheWriteThrough.Clear()
		redisCacheWriteBack.Clear()
		redisCacheRefreshAhead.Clear()

		json.NewEncoder(w).Encode(map[string]string{
			"message": "Semua data cache di Redis Container dibersihkan & hit counter di-reset",
		})
	})

	// Start server
	port := ":8080"
	fmt.Printf("[SERVER] Berjalan di port %s dengan REDIS CONTAINER dan MySQL Database.\n", port)
	fmt.Println("=== Available Endpoints ===")
	fmt.Println("1. Cache-Aside:          GET/POST /api/cache-aside")
	fmt.Println("2. Read-Through:         GET /api/read-through")
	fmt.Println("3. Write-Through:        GET/POST /api/write-through")
	fmt.Println("4. Write-Back:           GET/POST /api/write-back")
	fmt.Println("5. Write-Back Flush:     POST /api/write-back/flush")
	fmt.Println("6. Refresh-Ahead:        GET /api/refresh-ahead")
	fmt.Println("7. Large Data:           GET /api/large-data")
	fmt.Println("8. Cache Eviction:       POST /api/cache/evict?policy=lru|lfu|fifo")
	fmt.Println("9. Cache Validation:     GET /api/cache/validate?nim=xxx")
	fmt.Println("10. Reset Cache:         GET /api/reset")
	fmt.Println("===========================")

	if err := http.ListenAndServe(port, nil); err != nil {
		fmt.Printf("Gagal memulai server: %v\n", err)
	}
}
