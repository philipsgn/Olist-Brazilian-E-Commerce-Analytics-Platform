    -- =============================================
    -- KHỞI TẠO DATABASE CHO ECOMMERCE DE PROJECT
    -- Chạy tự động khi PostgreSQL start lần đầu
    -- =============================================

    -- Tạo schema RAW: chứa dữ liệu thô 1-1 từ nguồn
    CREATE SCHEMA IF NOT EXISTS raw;

    -- Tạo schema MARTS: chứa data đã transform, sẵn sàng phân tích
    CREATE SCHEMA IF NOT EXISTS marts;

    -- =============================================
    -- CÁC BẢNG TRONG SCHEMA RAW
    -- (copy y hệt cấu trúc file CSV, không thay đổi)
    -- =============================================

    CREATE TABLE IF NOT EXISTS raw.orders (
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

    CREATE TABLE IF NOT EXISTS raw.customers (
        customer_id               VARCHAR(50),
        customer_unique_id        VARCHAR(50),
        customer_zip_code_prefix  VARCHAR(10),
        customer_city             VARCHAR(100),
        customer_state            VARCHAR(5),
        loaded_at                 TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.order_items (
        order_id             VARCHAR(50),
        order_item_id        INTEGER,
        product_id           VARCHAR(50),
        seller_id            VARCHAR(50),
        shipping_limit_date  TIMESTAMP,
        price                NUMERIC(10,2),
        freight_value        NUMERIC(10,2),
        loaded_at            TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.order_payments (
        order_id              VARCHAR(50),
        payment_sequential    INTEGER,
        payment_type          VARCHAR(30),
        payment_installments  INTEGER,
        payment_value         NUMERIC(10,2),
        loaded_at             TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.order_reviews (
        review_id               VARCHAR(50),
        order_id                VARCHAR(50),
        review_score            INTEGER,
        review_comment_title    VARCHAR(100),
        review_comment_message  TEXT,
        review_creation_date    TIMESTAMP,
        review_answer_timestamp TIMESTAMP,
        loaded_at               TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.products (
        product_id                  VARCHAR(50),
        product_category_name       VARCHAR(100),
        product_name_lenght         INTEGER,
        product_description_lenght  INTEGER,
        product_photos_qty          INTEGER,
        product_weight_g            NUMERIC(10,2),
        product_length_cm           NUMERIC(10,2),
        product_height_cm           NUMERIC(10,2),
        product_width_cm            NUMERIC(10,2),
        loaded_at                   TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.sellers (
        seller_id                VARCHAR(50),
        seller_zip_code_prefix   VARCHAR(10),
        seller_city              VARCHAR(100),
        seller_state             VARCHAR(5),
        loaded_at                TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.geolocation (
        geolocation_zip_code_prefix  VARCHAR(10),
        geolocation_lat              NUMERIC(12,8),
        geolocation_lng              NUMERIC(12,8),
        geolocation_city             VARCHAR(100),
        geolocation_state            VARCHAR(5),
        loaded_at                    TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS raw.product_category_translation (
        product_category_name           VARCHAR(100),
        product_category_name_english   VARCHAR(100),
        loaded_at                       TIMESTAMP DEFAULT NOW()
    );

    -- Bảng lưu đơn hàng từ streaming (giả lập real-time)
    CREATE TABLE IF NOT EXISTS raw.streaming_orders (
        order_id        VARCHAR(50),
        customer_state  VARCHAR(5),
        product         VARCHAR(100),
        price           NUMERIC(10,2),
        payment_type    VARCHAR(30),
        created_at      TIMESTAMP,
        loaded_at       TIMESTAMP DEFAULT NOW()
    );

    -- Bảng lưu tỷ giá từ API
    CREATE TABLE IF NOT EXISTS raw.exchange_rates (
        fetched_at      TIMESTAMP,
        base_currency   VARCHAR(5),
        brl_to_usd      NUMERIC(10,6),
        vnd_to_usd      NUMERIC(15,10),
        loaded_at       TIMESTAMP DEFAULT NOW()
    );

    SELECT 'Database initialized successfully!' AS status;