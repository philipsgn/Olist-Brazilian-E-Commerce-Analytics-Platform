---
description: Clean-Deploy-EC2
---

[PURPOSE]
Tự động hóa quy trình dọn dẹp database lỗi cũ dính volume và deploy bản sạch lên EC2 qua Docker Compose.

[STEPS]
1. Kiểm tra các thay đổi trong file docker-compose.yml, Dockerfile.superset và .env ở Local.
2. Thực hiện chuỗi lệnh Git chuẩn để push code lên Repo:
   git add . && git commit -m "chore: production stack optimization" && git push origin main
3. Cung cấp chuỗi lệnh một chạm cho người dùng để copy dán vào Terminal EC2 nhằm dọn dẹp triệt để Volume cũ và build lại:
   cd ~/Olist-Brazilian-E-Commerce-Analytics-Platform && sudo docker compose down --volumes --remove-orphans && sudo docker volume rm $(sudo docker volume ls -q) 2>/dev/null || true && sudo docker compose up -d --build