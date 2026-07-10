package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Mahasiswa struct {
	NamaKelas    string `json:"namaKelas"`
	Nim          string `json:"nim"`
	Nama         string `json:"nama"`
	Email        string `json:"email"`
	EmailStudent string `json:"emailStudent"`
}

type DB struct {
	mu           sync.RWMutex
	mysqlDB      *sql.DB
	queryCounter int
}

func NewDB(mysqlDSN string) (*DB, error) {
	sqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping MySQL: %w", err)
	}

	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(5)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	db := &DB{
		mysqlDB: sqlDB,
	}
	if err := db.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return db, nil
}

func (db *DB) createTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS mahasiswa (
		nim VARCHAR(20) PRIMARY KEY,
		nama_kelas VARCHAR(50),
		nama VARCHAR(255),
		email VARCHAR(255),
		email_student VARCHAR(255),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`
	_, err := db.mysqlDB.Exec(query)
	return err
}

func (db *DB) Close() error {
	if db.mysqlDB != nil {
		return db.mysqlDB.Close()
	}
	return nil
}

func (db *DB) GetByNim(ctx context.Context, nim string) (Mahasiswa, error) {
	db.mu.Lock()
	db.queryCounter++
	db.mu.Unlock()

	var m Mahasiswa
	query := "SELECT nim, nama_kelas, nama, email, email_student FROM mahasiswa WHERE nim = ?"
	err := db.mysqlDB.QueryRowContext(ctx, query, nim).Scan(
		&m.Nim,
		&m.NamaKelas,
		&m.Nama,
		&m.Email,
		&m.EmailStudent,
	)

	if err == sql.ErrNoRows {
		return Mahasiswa{}, fmt.Errorf("mahasiswa dengan NIM %s tidak ditemukan", nim)
	}
	if err != nil {
		return Mahasiswa{}, fmt.Errorf("gagal query mahasiswa: %w", err)
	}

	return m, nil
}

func (db *DB) Save(ctx context.Context, m Mahasiswa) error {
	db.mu.Lock()
	db.queryCounter++
	db.mu.Unlock()

	query := `
		INSERT INTO mahasiswa (nim, nama_kelas, nama, email, email_student)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			nama_kelas = VALUES(nama_kelas),
			nama = VALUES(nama),
			email = VALUES(email),
			email_student = VALUES(email_student)
	`

	_, err := db.mysqlDB.ExecContext(ctx, query,
		m.Nim,
		m.NamaKelas,
		m.Nama,
		m.Email,
		m.EmailStudent,
	)

	if err != nil {
		return fmt.Errorf("gagal menyimpan mahasiswa: %w", err)
	}

	return nil
}

func (db *DB) ResetCounter() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.queryCounter = 0
}

func (db *DB) GetQueryCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.queryCounter
}

func (db *DB) GetAllLocal() []Mahasiswa {
	query := "SELECT nim, nama_kelas, nama, email, email_student FROM mahasiswa"
	rows, err := db.mysqlDB.Query(query)
	if err != nil {
		return []Mahasiswa{}
	}
	defer rows.Close()

	list := make([]Mahasiswa, 0)
	for rows.Next() {
		var m Mahasiswa
		if err := rows.Scan(&m.Nim, &m.NamaKelas, &m.Nama, &m.Email, &m.EmailStudent); err != nil {
			continue
		}
		list = append(list, m)
	}
	return list
}
