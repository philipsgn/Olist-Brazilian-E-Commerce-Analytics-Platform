# ⚡ Streaming Ingestion Pipeline

## 1. Architecture
Pipeline này mô phỏng luồng giao dịch thời gian thực của sàn thương mại điện tử:
`Lambda (Producer) -> Kinesis Data Firehose -> S3 Raw`

- **Lambda Producer:** Được lập lịch (EventBridge) để tạo ra các đơn hàng giả lập với Taxonomy chuẩn của Olist.
- **Kinesis Firehose:** Đóng vai trò là "Buffer". Nó thu thập các sự kiện nhỏ và gộp chúng lại (Batching) theo thời gian (60s) hoặc dung lượng (1MB) trước khi ghi vào S3 để tránh hiện tượng "Small Files Problem".

## 2. Transformation
- Lambda thực hiện chuẩn hóa dữ liệu ngay lúc gửi (Timestamp format, Source tagging).
- Firehose tự động phân vùng dữ liệu trên S3 theo cấu trúc `year=YYYY/month=MM/day=DD/hour=HH`.

## 3. 📸 Screenshots (Hành động: Chèn ảnh của bạn vào đây)
- **Kinesis Firehose Stream:** `![Firehose Monitoring](../images/screenshot_firehose.png)`
- **Lambda Function Code/Test:** `![Lambda Producer](../images/screenshot_lambda.png)`
- **CloudWatch Logs:** `![Ingestion Logs](../images/screenshot_cloudwatch.png)`
