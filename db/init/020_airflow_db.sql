-- ============================================
-- 01_init.sql
-- Chạy tự động khi PostgreSQL khởi động lần đầu
-- ============================================

-- Tạo database riêng cho Airflow
CREATE DATABASE airflow;

-- Kết nối vào ecommerce_db để tạo schemas
\c ecommerce_db;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

SELECT 'Initialized successfully!' AS status;