terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ── Provider trỏ vào Floci (giả lập S3) ──────────────────────────────────────
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true 

  endpoints {
    s3 = "http://localhost:4566"
  }
}

# ── S3 Bucket chính (logistics-lake) ─────────────────────────────────────────
resource "aws_s3_bucket" "logistics_lake" {
  bucket        = "logistics-lake"
  force_destroy = true

  tags = {
    Project = "logistics-etl"
    Layer   = "all"
  }
}

# Tắt Block Public Access (cần thiết cho môi trường local)
resource "aws_s3_bucket_public_access_block" "logistics_lake" {
  bucket = aws_s3_bucket.logistics_lake.id

  block_public_acls       = false
  block_public_policy     = false
  ignore_public_acls      = false
  restrict_public_buckets = false
}

# ── Tạo thư mục "prefix" cho 3 layer ─────────────────────────────────────────
resource "aws_s3_object" "bronze_prefix" {
  bucket  = aws_s3_bucket.logistics_lake.id
  key     = "bronze/"
  content = ""
}

resource "aws_s3_object" "silver_prefix" {
  bucket  = aws_s3_bucket.logistics_lake.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold_prefix" {
  bucket  = aws_s3_bucket.logistics_lake.id
  key     = "gold/"
  content = ""
}

# ── Output ────────────────────────────────────────────────────────────────────
output "bucket_name" {
  value = aws_s3_bucket.logistics_lake.bucket
}

output "s3_endpoint" {
  value = "http://localhost:4566"
}
