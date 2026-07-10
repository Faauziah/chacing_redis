
-- Buat database jika belum ada
CREATE DATABASE IF NOT EXISTS chacheredis CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Gunakan database
USE chacheredis;

-- Tabel akan dibuat otomatis oleh aplikasi Go
-- Namun jika ingin membuat manual, gunakan query berikut:

/*
CREATE TABLE IF NOT EXISTS mahasiswa (
    nim VARCHAR(20) PRIMARY KEY,
    nama_kelas VARCHAR(50),
    nama VARCHAR(255),
    email VARCHAR(255),
    email_student VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/
