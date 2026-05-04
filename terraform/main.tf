provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  description = "AWS Region"
  default     = "ap-southeast-1"
}

variable "bucket_name" {
  description = "The name of the S3 bucket for the Olist data lake"
  type        = string
  default     = "olist-de-tanphat-2026" # TODO: Đổi tên bucket của bạn ở đây
}

# Tham chiếu đến bucket hiện có (hoặc tạo mới nếu bạn dùng aws_s3_bucket)
# Nếu bucket đã tồn tại, bạn có thể dùng data source:
# data "aws_s3_bucket" "existing_bucket" {
#   bucket = var.bucket_name
# }
# Và dùng data.aws_s3_bucket.existing_bucket.id trong aws_s3_bucket_lifecycle_configuration

# Giả sử bạn tạo mới hoặc quản lý bucket này qua Terraform
resource "aws_s3_bucket" "data_lake_bucket" {
  bucket = var.bucket_name
}

# Cấu hình Lifecycle Policy — bao phủ đúng thư mục chứa data thật
resource "aws_s3_bucket_lifecycle_configuration" "data_lifecycle" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  # Rule 1: Dữ liệu đã xử lý (Parquet) ở tầng Processed — Airflow export mỗi ngày
  rule {
    id     = "archive-processed-layer"
    status = "Enabled"

    filter {
      prefix = "processed/"
    }

    # Sau 30 ngày: Chuyển sang S3 Standard-Infrequent Access (ít truy cập, tiết kiệm ~60%)
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # Sau 90 ngày: Chuyển sang Glacier Flexible Retrieval (lưu trữ lâu dài, tiết kiệm ~80%)
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }

  # Rule 2: Dữ liệu thô CSV và Streaming ở tầng Raw — ít khi cần truy cập lại
  rule {
    id     = "archive-raw-layer"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    # Raw data ít quan trọng hơn, chuyển sang IA sớm hơn (60 ngày)
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }

    # Sau 180 ngày (6 tháng): Đưa vào Glacier để lưu trữ tuân thủ
    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}
