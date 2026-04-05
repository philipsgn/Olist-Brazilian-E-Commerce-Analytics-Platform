# 📘 Data Engineering Project Handbook

Dự án này là một nền tảng **End-to-End E-commerce Analytics**, được xây dựng trên bộ công cụ **Modern Data Stack (MDS)**. Bản hướng dẫn này giúp bạn nắm bắt toàn bộ tư duy và quy trình đã thiết kế cho dự án.

## 🏗️ 1. Kiến trúc Tổng quan (The Stack)
1. **Dữ liệu nguồn (Sources)**: 9 tệp CSV từ Kaggle (Olist Brazilian E-commerce).
2. **Hệ Quản trị CSDL (Warehouse)**: PostgreSQL 15, phân làm 3 schemas (Raw, Staging, Marts).
3. **Điều phối & Tự động hóa (Orchestration)**: Apache Airflow 2.9 (Dockerized).
4. **Biến đổi Dữ liệu (Modeling)**: dbt-core 1.11 (với SQL-based transformations).
5. **Trực quan hóa (BI)**: Apache Superset.

---

## 🛠️ 2. Quy trình DE thực tế (Workflow)

### Giai đoạn 1: Ingestion (EL - Extract & Load)
- **Công cụ**: Python script (`load_csv.py`) kết hợp với Airflow `PythonOperator`.
- **Logic**: Dùng thư viện `pandas` để đọc tệp CSV lớn, chuẩn hóa tên cột thô, và nạp (Append) vào các bảng trong `raw` schema.
- **Tư duy Senior**: Chúng ta không đổi tên hay sửa logic ở đây. Đây là nơi lưu giữ dữ liệu gốc để có thể phục hồi (auditability) nếu sau này phát hiện lỗi logic ở tầng sau.

### Giai đoạn 2: Data Modeling (T - Transform)
Đây là "trái tim" của dự án, dùng **dbt** để triển khai mô hình đa tầng:
1. **Lớp Staging**: Tạo ra các View (`stg_orders`, `stg_products`,...). Tại đây chúng ta Cast dữ liệu (VD: String sang Timestamp), đổi danh mục sản phẩm từ tiếng Bồ Đào Nha sang tiếng Anh.
2. **Lớp Marts**: Xây dựng mô hình **Star Schema** (Fact & Dimension Tables):
   - **Fact Table**: `fact_order_items`, `fact_payments`. Chứa các con số và khóa ngoại.
   - **Dim Table**: `dim_customers`, `dim_products`. Chứa thông tin mô tả.
- **Tối ưu hóa**:
  - **Incremental**: Giúp hệ thống chạy cực nhanh vì mỗi ngày dbt chỉ nạp "phần mới" (delta nạp) thay vì nạp lại toàn bộ.
  - **Indexing**: Tạo B-Tree indexes trên PostgreSQL để dashboard Superset phản hồi dưới 1 giây.

### Giai đoạn 3: Data Quality (DQ)
- **Cơ chế**: Dùng dbt tests (`unique`, `not_null`, `accepted_values`) được cấu hình trong `schema.yml`.
- **Quản lý lỗi**: Trong Airflow, nếu một bài test dbt fail, pipeline sẽ **dừng lại ngay lập tức** (Gated Pipeline). Điều này giúp dữ liệu sai không bao giờ lọt lên tới Dashboard của sếp.

---

## 🛰️ 3. Quy trình CI/CD chuyên nghiệp
Chúng ta dùng **GitHub Actions** để tự động hóa:
1. Mỗi khi có code mới được Push lên GitHub, một máy ảo (Runner) sẽ tự động bật Database Postgres tạm thời.
2. Nó chạy lệnh `dbt debug` và `dbt compile`.
3. Nếu code SQL của bạn bị lỗi cú pháp, hệ thống sẽ báo đỏ, không cho phép cập nhật lên Production. Đây là tiêu chuẩn vàng của **DataOps**.

---

## 🎯 4. Các câu hỏi phỏng vấn "vàng" (Interview Tips)

- **Hỏi: Tại sao em dùng Airflow mà không dùng Cron Job?**
  - Trả lời: *"Airflow cho phép quản lý các Task phụ thuộc (Dependency) cực tốt, có cơ chế Retry tự động nếu DB bị treo, và có giao diện monitor trực quan giúp mình biết chính xác step nào đang lỗi."*

- **Hỏi: Em xử lý thế nào nếu kích thước dữ liệu tăng gấp 10 lần?**
  - Trả lời: *"Em đã triển khai Incremental Loading trong dbt để chỉ xử lý dữ liệu mới phát sinh. Ngoài ra em còn tối ưu hóa Indexing và Pre-aggregation ở tầng Fact để giảm tải cho tính toán Join."*

- **Hỏi: Em đảm bảo chất lượng dữ liệu bằng cách nào?**
  - Trả lời: *"Em thực hiện kiểm thử tự động ở cấp độ schema và logic sau mỗi tầng biến đổi bằng dbt tests. Pipeline của em là một Gated Pipeline - dừng lại nếu có lỗi nghiêm trọng ở Staging cửa ngõ."*
