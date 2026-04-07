-- =============================================
-- db/init/010_ecommerce_db.sql
-- Postgres chạy file này ĐẦU TIÊN khi khởi tạo container.
-- Nội dung: Tạo raw/marts schema + tất cả bảng dữ liệu.
--
-- File gốc: ingestion/init.sql
-- Được copy vào đây để Postgres auto-run qua docker-entrypoint-initdb.d
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

-- 3. Tạo các bảng với tên KHỚP HOÀN TOÀN với script Python (load_csv.py)

CREATE TABLE raw.orders (
    order_id                         VARCHAR(50),
    customer_id                      VARCHAR(50),
    order_status                     VARCHAR(20),
    order_purchase_timestamp         TIMESTAMP,
    order_approved_at                TIMESTAMP,
    order_delivered_carrier_date     TIMESTAMP,
    order_delivered_customer_date    TIMESTAMP,
    order_estimated_delivery_date    TIMESTAMP,
    loaded_at                        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.customers (
    customer_id               VARCHAR(50),
    customer_unique_id        VARCHAR(50),
    customer_zip_code_prefix  VARCHAR(10),
    customer_city             VARCHAR(100),
    customer_state            VARCHAR(5),
    loaded_at                 TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.order_items (
    order_id             VARCHAR(50),
    order_item_id        INTEGER,
    product_id           VARCHAR(50),
    seller_id            VARCHAR(50),
    shipping_limit_date  TIMESTAMP,
    price                NUMERIC(10,2),
    freight_value        NUMERIC(10,2),
    loaded_at            TIMESTAMP DEFAULT NOW()
);

CREATE TABLE raw.payments (
    order_id              VARCHAR(50),
    payment_sequential    INTEGER,
    payment_type          VARCHAR(30),
    payment_installments  INTEGER,
    payment_value         NUMERIC(10,2),
    loaded_at             TIMESTAMP DEFAULT NOW()
);

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

CREATE TABLE raw.category_translation (
    product_category_name          VARCHAR(100),
    product_category_name_english  VARCHAR(100),
    loaded_at                      TIMESTAMP DEFAULT NOW()
);

SELECT 'ecommerce_db: raw schema tables created successfully!' AS status;
