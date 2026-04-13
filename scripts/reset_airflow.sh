#!/bin/bash

# =============================================================================
# SCRIPT: reset_airflow.sh
# MỤC TIÊU: Hard Reset toàn bộ hệ thống Clean 100%
# =============================================================================

echo "🚀 Bắt đầu quy trình Hard Reset Airflow (Production Standard)..."

# 1. Dọn dẹp mạnh tay
echo "⏳ 1. Đang gỡ bỏ toàn bộ container và xóa dữ liệu cũ..."
docker compose down -v
# Xóa thủ công nếu còn kẹt (đảm bảo không bị lỗi Conflict Name)
docker rm -f de_postgres de_redis de_airflow_webserver de_airflow_scheduler de_airflow_worker de_pgadmin de_superset 2>/dev/null || true

# 2. Kiểm tra Python Package
echo "⏳ 2. Đảm bảo tính toàn vẹn của mô-đun Python..."
touch airflow/utils/__init__.py
echo "✅ Đã kiểm tra airflow/utils/__init__.py"

# 3. Rebuild (Sử dụng config mới nhất từ docker-compose.yml)
echo "⏳ 3. Đang Rebuild Docker Images..."
docker compose build --no-cache

# 4. Khởi động Infrastructure
echo "⏳ 4. Đang khởi động Database & Redis..."
docker compose up -d postgres redis
echo "   - Chờ Healthcheck (15s)..."
sleep 15

# 5. Khởi động App Layer (Dùng SERVICE NAME chuẩn)
echo "⏳ 5. Đang khởi động Webserver & Scheduler..."
docker compose up -d airflow-webserver airflow-scheduler
echo "   - Chờ khởi tạo Database (15s)..."
sleep 15

# 6. Khởi động Worker
echo "⏳ 6. Đang khởi động Worker layer..."
docker compose up -d airflow-worker

echo "======================================================"
echo "✅ QUY TRÌNH HOÀN TẤT!"
echo "Hãy kiểm tra bằng lệnh: docker ps"
echo "Truy cập: http://<EC2-IP>:8080"
echo "======================================================"
