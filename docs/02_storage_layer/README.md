# 📦 Storage Layer & Data Lake Strategy

## 1. Medallion Architecture
Dữ liệu được tổ chức theo cấu trúc 3 tầng chuẩn industry:
- **Bronze (Raw):** Giữ nguyên định dạng gốc (JSON cho Streaming, CSV cho Batch). Đây là nguồn tin cậy duy nhất (Single Source of Truth).
- **Silver (Staging):** Dữ liệu đã được làm sạch, xử lý kiểu dữ liệu qua dbt.
- **Gold (Processed):** Dữ liệu đã được nén **Snappy** và lưu dưới dạng **Parquet**. Đây là định dạng cột (columnar) giúp Athena truy vấn nhanh và tiết kiệm chi phí nhất.

## 2. Cost Optimization (FinOps)
- **S3 Lifecycle Management:** 
  - Sau 30-60 ngày: Dữ liệu Raw ít khi được truy cập sẽ được chuyển tự động sang **S3 Standard-IA**.
  - Sau 180 ngày: Chuyển sang **S3 Glacier Flexible Retrieval** để lưu trữ dài hạn với chi phí cực thấp.
- **Compression:** Việc chuyển từ CSV sang Parquet Snappy giúp giảm dung lượng lưu trữ tới 70-80%.

## 3. 📸 Screenshots (Hành động: Chèn ảnh của bạn vào đây)
- **S3 Bucket Structure:** `![S3 Folders](../images/screenshot_s3_structure.png)`
- **S3 Lifecycle Configuration:** `![Lifecycle Policy](../images/screenshot_s3_lifecycle.png)`
- **Parquet Files in S3:** `![Parquet Files](../images/screenshot_s3_parquet.png)`
