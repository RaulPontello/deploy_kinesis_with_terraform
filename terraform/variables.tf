# ─────────────────────────────────────────────
#  Generic variables
# ─────────────────────────────────────────────

variable "project_name" {
  description = "Name used to prefix all AWS resources"
  type        = string
  default     = "stock-market-kinesis"
}

variable "profile_name" {
  description = "Name of the profile used to connect to AWS Account"
  type        = string
  default     = "caylent_profile"
}

variable "owner_name" {
  description = "Name of the project's owner"
  type        = string
  default     = "raul.pontello@caylent.com"
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

# ─────────────────────────────────────────────
#  S3 Bucket
# ─────────────────────────────────────────────

variable "s3_bucket_force_destroy" {
  description = "Allow Terraform to destroy the S3 bucket even if it contains objects (use true only for dev)"
  type        = bool
  default     = true
}

# ─────────────────────────────────────────────
#  Kinesis Data Stream
# ─────────────────────────────────────────────

variable "kinesis_shard_count" {
  description = <<-EOT
    Number of shards for the Kinesis Data Stream.
    Each shard supports 1 MB/s ingestion and 2 MB/s consumption.
    Rule of thumb: 1 shard per 1 MB/s of expected throughput.
  EOT
  type        = number
  default     = 1
}

variable "kinesis_retention_hours" {
  description = "Data retention period in hours (24–8760). Default is 24 h."
  type        = number
  default     = 24

  validation {
    condition     = var.kinesis_retention_hours >= 24 && var.kinesis_retention_hours <= 8760
    error_message = "Retention must be between 24 and 8760 hours."
  }
}

variable "kinesis_encryption_enabled" {
  description = "Enable server-side encryption on the Kinesis stream using AWS-managed KMS key"
  type        = bool
  default     = true
}

# ─────────────────────────────────────────────
#  Kinesis Data Firehose
# ─────────────────────────────────────────────

variable "firehose_buffer_size_mb" {
  description = "Buffer size (MB) before Firehose flushes data to S3. Range: 1–128."
  type        = number
  default     = 5
}

variable "firehose_buffer_interval_seconds" {
  description = "Buffer interval (seconds) before Firehose flushes data to S3. Range: 60–900."
  type        = number
  default     = 300
}

variable "firehose_s3_compression" {
  description = "Compression format for S3 objects (UNCOMPRESSED | GZIP | ZIP | Snappy)"
  type        = string
  default     = "GZIP"
}

# ─────────────────────────────────────────────
#  Lambda – Spike Detector
# ─────────────────────────────────────────────

variable "anomaly_threshold_pct" {
  description = "Absolute price-change percentage that triggers a spike alert (e.g. 5.0 means ±5%)"
  type        = number
  default     = 1
}

variable "alert_email" {
  description = "Email address that receives SNS spike-alert notifications"
  type        = string
  default     = "raul.pontello@caylent.com"
}
