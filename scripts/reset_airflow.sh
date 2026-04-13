#!/bin/bash

# =================================================================
# Script: reset_airflow.sh
# Purpose: Hard reset hạ tầng Airflow, xóa task kẹt và rebuild code.
# Author: Senior DevOps/Data Engineer
# =================================================================

# 1. Định nghĩa màu sắc cho log
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}🚀 Bắt đầu quy trình Hard Reset Airflow...${NC}"

# 2. Dừng toàn bộ Containers và xóa Volumes (Xóa sạch task kẹt trong Redis/Postgres)
echo -e "${YELLOW}⏳ 1. Đang dừng containers và gỡ bỏ volumes...${NC}"
docker compose down -v

# 3. Tạo file __init__.py nếu chưa có (Tránh lỗi ModuleNotFoundError)
echo -e "${YELLOW}⏳ 2. Đảm bảo tính toàn vẹn của mô-đun Python...${NC}"
touch airflow/utils/__init__.py
echo -e "${GREEN}✅ Đã kiểm tra airflow/utils/__init__.py${NC}"

# 4. Ép build lại Image (Không dùng cache để cập nhật code discord_alerts.py mới nhất)
echo -e "${YELLOW}⏳ 3. Đang Rebuild Docker Images (no-cache)...${NC}"
docker compose build --no-cache

# 5. Khởi động lại theo thứ tự ưu tiên
echo -e "${YELLOW}⏳ 4. Đang khởi động Infrastructure (Database & Redis)...${NC}"
docker compose up -d postgres redis
echo -e "${YELLOW}   - Chờ Database và Redis sẵn sàng (15s)...${NC}"
sleep 15

echo -e "${YELLOW}⏳ 5. Đang khởi động App Layer (Webserver & Scheduler)...${NC}"
docker compose up -d airflow-webserver airflow-scheduler
echo -e "${YELLOW}   - Chờ Webserver khởi tạo DB (10s)...${NC}"
sleep 10

echo -e "${YELLOW}⏳ 6. Đang khởi động Worker layer...${NC}"
docker compose up -d airflow-worker

# 6. Kiểm tra trạng thái cuối cùng
echo -e "${GREEN}======================================================${NC}"
echo -e "${GREEN}✅ QUY TRÌNH HOÀN TẤT!${NC}"
echo -e "${GREEN}Hãy kiểm tra trạng thái container bằng lệnh: docker ps${NC}"
echo -e "${GREEN}Truy cập Airflow UI tại: http://<EC2-IP>:8080${NC}"
echo -e "${GREEN}======================================================${NC}"
