-- =============================================
-- KHỞI TẠO AIRFLOW METADATA DATABASE
-- File: ingestion/init_airflow.sql
--
-- MỤC ĐÍCH:
--   Tạo database riêng cho Airflow metadata (không dùng chung
--   với ecommerce_db). Script này chạy 1 lần khi Postgres
--   container khởi động lần đầu tiên (via docker-entrypoint-initdb.d).
--
-- THỨ TỰ CHẠY (trong db/init/):
--   010_ecommerce_db.sql  → Tạo raw schema + bảng dữ liệu
--   020_init_airflow.sql  → Tạo Airflow database + cấu hình
-- =============================================

-- ─────────────────────────────────────────────
-- BƯỚC 1: Tạo Airflow database (nếu chưa có)
-- Lưu ý: CREATE DATABASE không chạy được trong transaction block.
-- PostgreSQL docker entrypoint chạy mỗi file trong transaction
-- riêng, nên lệnh này an toàn khi đặt ở đầu file.
-- ─────────────────────────────────────────────
SELECT 'CREATE DATABASE airflow OWNER de_user'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec


-- ─────────────────────────────────────────────
-- BƯỚC 2: Kết nối vào airflow database để setup
-- ─────────────────────────────────────────────
\connect airflow


-- ─────────────────────────────────────────────
-- BƯỚC 3: Cấp quyền đầy đủ cho de_user trên airflow db
-- (Airflow tự tạo bảng qua `airflow db migrate`, ta chỉ cần
--  đảm bảo user có quyền)
-- ─────────────────────────────────────────────
GRANT ALL PRIVILEGES ON DATABASE airflow TO de_user;
GRANT ALL ON SCHEMA public TO de_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO de_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO de_user;


-- ─────────────────────────────────────────────
-- BƯỚC 4: Pre-seed Airflow Variables
-- Các biến này sẽ được dùng trong DAG ecommerce_pipeline.py
-- Lưu ý: Bảng 'variable' chưa tồn tại ở đây vì Airflow chưa
--         chạy `db migrate`. Ta tạo bảng tạm để seed, sau khi
--         airflow migrate, nó sẽ merge tự động.
--
--   ⚠️  CÁCH THỰC TẾ: Dùng Airflow CLI hoặc UI để set variable.
--       Script này chỉ làm tài liệu hóa các variable cần thiết.
-- ─────────────────────────────────────────────

-- Tạo bảng ghi chú các Airflow Variables cần thiết
-- (Đây là bảng tracking riêng, không phải bảng Airflow thật)
CREATE TABLE IF NOT EXISTS public.pipeline_bootstrap_notes (
    variable_key     VARCHAR(250) PRIMARY KEY,
    variable_value   TEXT,
    description      TEXT,
    created_at       TIMESTAMP DEFAULT NOW()
);

INSERT INTO public.pipeline_bootstrap_notes (variable_key, variable_value, description) VALUES
    ('ENVIRONMENT',         'dev',
     'Môi trường chạy pipeline. Giá trị: dev | prod. DAG dùng biến này để chọn dbt target.'),
    ('DB_URI_LOCAL',        'postgresql://de_user:de_password@postgres:5432/ecommerce_db',
     'Connection string tới Postgres local (Docker). Dùng trong load_csv.py khi chạy qua Airflow.'),
    ('DATA_DIR',            '/opt/airflow/data',
     'Đường dẫn thư mục chứa CSV files trong Docker container (mount từ ./data trên host).'),
    ('DBT_PROJECT_DIR',     '/opt/airflow/dbt_project/ecommerce',
     'Đường dẫn dbt project directory trong container.'),
    ('DBT_PROFILES_DIR',    '/opt/airflow/dbt_project',
     'Đường dẫn thư mục chứa profiles.yml cho dbt.'),
    ('INGESTION_SCRIPT',    '/opt/airflow/ingestion/load_csv.py',
     'Đường dẫn tuyệt đối tới script ingestion trong container.'),
    ('SIMULATE_NEW_ORDERS', '100',
     'Số đơn hàng giả lập được tạo mỗi ngày bởi task generate_fake_data.')
ON CONFLICT (variable_key) DO UPDATE
    SET variable_value = EXCLUDED.variable_value,
        description    = EXCLUDED.description;

-- ─────────────────────────────────────────────
-- BƯỚC 5: Pre-seed Airflow Connections (tài liệu hóa)
-- Bảng 'connection' cũng chưa tồn tại ở bước này.
-- Dùng bảng notes để document các connection cần tạo trong UI.
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.pipeline_connection_notes (
    conn_id      VARCHAR(250) PRIMARY KEY,
    conn_type    VARCHAR(50),
    host         VARCHAR(500),
    schema_name  VARCHAR(100),
    login        VARCHAR(100),
    description  TEXT,
    created_at   TIMESTAMP DEFAULT NOW()
);

INSERT INTO public.pipeline_connection_notes
    (conn_id, conn_type, host, schema_name, login, description) VALUES
    ('postgres_ecommerce', 'postgres',
     'postgres', 'ecommerce_db', 'de_user',
     'Kết nối tới PostgreSQL local (Docker). Password: de_password. Port: 5432.'),
    ('aws_rds_production', 'postgres',
     'olist-de-db.c34iaimu4kqz.ap-southeast-1.rds.amazonaws.com',
     'ecommerce_db', 'de_user',
     'Kết nối tới AWS RDS PostgreSQL (Production). SSL: verify-full. Dùng global-bundle.pem.')
ON CONFLICT (conn_id) DO NOTHING;


-- ─────────────────────────────────────────────
-- BƯỚC 6: Kiểm tra kết quả
-- ─────────────────────────────────────────────
SELECT
    'Airflow DB initialized' AS status,
    COUNT(*) AS variables_documented
FROM public.pipeline_bootstrap_notes;
