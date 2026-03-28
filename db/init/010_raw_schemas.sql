-- Tạo 2 schema để tổ chức dữ liệu rõ ràng
-- raw   = dữ liệu thô, chưa xử lý, y hệt nguồn gốc
-- marts = dữ liệu đã xử lý, sẵn sàng để phân tích

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS marts;

-- Thông báo cho biết đã chạy xong
SELECT 'Schemas created successfully!' AS status;