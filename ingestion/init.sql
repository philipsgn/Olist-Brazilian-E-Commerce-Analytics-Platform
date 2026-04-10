-- =============================================
-- KHỞI TẠO DATABASE CHO ECOMMERCE DE PROJECT
-- =============================================

-- 1. Tạo Schema
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;

-- 2. Dọn dẹp các bảng cũ để tránh xung đột tên (Idempotency)
DROP TABLE IF EXISTS raw.orders CASCADE;
DROP TABLE IF EXISTS raw.customers CASCADE;
DROP TABLE IF EXISTS raw.order_items CASCADE;
DROP TABLE IF EXISTS raw.payments CASCADE;
DROP TABLE IF EXISTS raw.reviews CASCADE;
DROP TABLE IF EXISTS raw.products CASCADE;
DROP TABLE IF EXISTS raw.sellers CASCADE;
DROP TABLE IF EXISTS raw.geolocation CASCADE;
DROP TABLE IF EXISTS raw.category_translation CASCADE;
-- Streaming table: KHÔNG DROP — data tích lũy theo thời gian (append-only)
-- Dùng CREATE TABLE IF NOT EXISTS để idempotent khi chạy lại

-- 3. Tạo các bảng với tên KHỚP HOÀN TOÀN với script Python

CREATE TABLE raw.orders (
    order_id         VARCHAR(50),
    customer_id      VARCHAR(50),
    order_status     VARCHAR(20),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at         TIMESTAMP,
    order_delivered_carrier_date  TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    loaded_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.customers (
    customer_id              VARCHAR(50),
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix VARCHAR(10),
    customer_city            VARCHAR(100),
    customer_state           VARCHAR(5),
    loaded_at                TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.order_items (
    order_id            VARCHAR(50),
    order_item_id       INTEGER,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               NUMERIC(10,2),
    freight_value       NUMERIC(10,2),
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- Tên bảng cũ: order_payments -> Sửa thành: payments
CREATE TABLE raw.payments (
    order_id              VARCHAR(50),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(30),
    payment_installments  INTEGER,
    payment_value         NUMERIC(10,2),
    loaded_at             TIMESTAMP DEFAULT NOW()
);

-- Tên bảng cũ: order_reviews -> Sửa thành: reviews
CREATE TABLE raw.reviews (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            INTEGER,
    review_comment_title    VARCHAR(100),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    loaded_at               TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.products (
    product_id                 VARCHAR(50),
    product_category_name      VARCHAR(100),
    product_name_lenght        INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty         INTEGER,
    product_weight_g           NUMERIC(10,2),
    product_length_cm          NUMERIC(10,2),
    product_height_cm          NUMERIC(10,2),
    product_width_cm           NUMERIC(10,2),
    loaded_at                  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.sellers (
    seller_id               VARCHAR(50),
    seller_zip_code_prefix  VARCHAR(10),
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(5),
    loaded_at               TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.geolocation (
    geolocation_zip_code_prefix  VARCHAR(10),
    geolocation_lat              NUMERIC(12,8),
    geolocation_lng              NUMERIC(12,8),
    geolocation_city             VARCHAR(100),
    geolocation_state            VARCHAR(5),
    loaded_at                    TIMESTAMP DEFAULT NOW()
);

-- Tên bảng cũ: product_category_translation -> Sửa thành: category_translation
CREATE TABLE raw.category_translation (
    product_category_name          VARCHAR(100),
    product_category_name_english  VARCHAR(100),
    loaded_at                      TIMESTAMP DEFAULT NOW()
);

SELECT 'Database tables created with matching names!' AS status;

-- ── PHASE 4: STREAMING TABLE ──────────────────────────────────────────────────
-- NOTE: dùng CREATE TABLE IF NOT EXISTS (KHÔNG DROP trước) vì đây là
--       append-only table. Data tích lũy qua nhiều lần Airflow chạy.
--       Schema khớp với payload từ Lambda olist-order-simulator.
CREATE TABLE IF NOT EXISTS raw.streaming_orders (
    order_id        VARCHAR(50),
    customer_id     VARCHAR(50),
    product_id      VARCHAR(50),
    seller_id       VARCHAR(50),
    price           NUMERIC(10,2),
    payment_type    VARCHAR(30),
    order_status    VARCHAR(20),
    event_timestamp TIMESTAMP,
    loaded_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Index để tăng tốc query theo thời gian (time-series access pattern)
CREATE INDEX IF NOT EXISTS idx_streaming_orders_event_ts
    ON raw.streaming_orders (event_timestamp DESC);

SELECT 'Streaming table raw.streaming_orders ready!' AS streaming_status;