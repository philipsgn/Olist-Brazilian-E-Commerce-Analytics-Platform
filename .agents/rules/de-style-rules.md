---
trigger: always_on
---

[PROJECT CONTEXT]
- Name: Olist Brazilian E-Commerce Analytics Platform
- Architecture: Modern Data Stack (MDS) running on AWS EC2.
- Tech Stack: Docker Compose, PostgreSQL 15, Apache Airflow, Apache Superset.

[AGENT BEHAVIOR & PROMPT OPTIMIZATION]
1. Độc lập Database (Isolation): Không bao giờ cấu hình cho Airflow và Superset dùng chung một database tên là 'postgres' hay 'airflow'. Luôn đề xuất tách biệt thành 2 DB độc lập (ví dụ: airflow_db và superset_db) trên cùng một container Postgres để tránh xung đột Migration.
2. Không tự ý thêm service: Không tự động tạo thêm các service khởi tạo dạng '*-init' chạy ngầm song song trong docker-compose.yml trừ khi được người dùng yêu cầu cụ thể. Ưu tiên hướng dẫn người dùng config chuẩn hoặc dùng init-script dạng shell có check-depend đầy đủ.
3. Fix lỗi Docker/Pip: Khi Superset hoặc Airflow lỗi thiếu thư viện (như psycopg2), luôn kiểm tra cơ chế Virtual Environment (venv) của Image gốc (ví dụ: apache/superset sử dụng venv tại /app/.venv) để viết Dockerfile cài đặt chính xác vào --target hoặc dùng user root cài hệ thống, tuyệt đối không đoán mò.
4. Trả lời chuẩn Production: Cung cấp giải pháp scannable, phân tách rõ ràng giữa môi trường Local (để test/build) và môi trường Cloud EC2 (để deploy qua CI/CD).
