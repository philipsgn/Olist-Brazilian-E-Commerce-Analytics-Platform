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

# Cấu hình Lifecycle Policy cho thư mục Gold
resource "aws_s3_bucket_lifecycle_configuration" "gold_data_lifecycle" {
  bucket = aws_s3_bucket.data_lake_bucket.id

  rule {
    id     = "archive-gold-layer-data"
    status = "Enabled"

    filter {
      prefix = "gold/view_order_analytics/"
    }

    # Sau 30 ngày: Chuyển sang S3 Standard-Infrequent Access
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # Sau 90 ngày: Chuyển sang Glacier Flexible Retrieval
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}
