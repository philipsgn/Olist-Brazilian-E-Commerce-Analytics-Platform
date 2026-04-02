# =============================================================================
# FILE: airflow/Dockerfile
# =============================================================================
# WHY DOCKERFILE instead of "pip install" in docker-compose command?
# -----------------------------------------------------------------
# Cách cũ (pip install khi startup) có 2 vấn đề:
#   1. Mỗi lần container restart → phải chờ pip install lại (~2-3 phút)
#   2. pip install on top của môi trường đang chạy → có thể upgrade/downgrade
#      các packages sẵn có → gây conflicts (đúng như lỗi bạn thấy với click)
#
# Cách mới (Dockerfile):
#   1. pip install chạy 1 lần duy nhất khi BUILD image
#   2. pip giải quyết TẤT CẢ dependencies cùng lúc → ít conflicts hơn
#   3. Container start nhanh hơn (không cần install gì thêm)
#   4. Image có thể share với team, ai cũng dùng cùng một môi trường
#
# Đây là cách ĐÚNG trong production. Dockerfile là "công thức" build image.
# =============================================================================

# Bắt đầu từ image Airflow chính thức
# "FROM" = "Kế thừa tất cả mọi thứ từ image này"
FROM apache/airflow:2.9.3

# Switch sang user "airflow" trước khi pip install
# WHY? Airflow image dùng user "airflow" (không phải root) vì lý do bảo mật.
# Nếu pip install với root, file sẽ thuộc root → airflow user không dùng được.
USER airflow

# Cài dbt-postgres
# --no-cache-dir: Không lưu cache pip → image nhẹ hơn
# dbt-postgres 1.8.7 là version stable, tương thích với Airflow 2.9.3
# (dbt-postgres tự động kéo theo dbt-core cùng version)
#
# WHY 1.8.9 thay vì 1.10.0/1.11.7?
# Airflow 2.9.3 có constraint về click và một số package.
# dbt-core 1.8.x đã được test và confirm tương thích.
# dbt project của bạn (models SQL) hoàn toàn chạy được trên 1.8.9.
RUN pip install --no-cache-dir \
    "dbt-core>=1.8.0,<1.9.0" \
    "dbt-postgres>=1.8.0,<1.9.0"
