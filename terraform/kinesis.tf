# ═══════════════════════════════════════════════════════════════════════════
#  main.tf  –  Stock Market Kinesis Pipeline
#
#  Architecture:
#    Python Producer (yfinance)
#        │
#        ▼
#    Kinesis Data Stream  (real-time ingestion)
#        │
#        ▼
#    Kinesis Data Firehose  (buffering + delivery)
#        │
#        ▼
#    S3 Data Lake  (durable storage, partitioned by year/month/day/hour)
# ═══════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────
#  Kinesis Data Stream
# ─────────────────────────────────────────────

resource "aws_kinesis_stream" "stock_market" {
  name             = local.kinesis_stream_name
  shard_count      = var.kinesis_shard_count
  retention_period = var.kinesis_retention_hours
  encryption_type  = var.kinesis_encryption_enabled ? "KMS" : "NONE"
  kms_key_id       = var.kinesis_encryption_enabled ? "alias/aws/kinesis" : null

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# ─────────────────────────────────────────────
# Kinesis Data Firehose
# ─────────────────────────────────────────────

resource "aws_kinesis_firehose_delivery_stream" "to_s3" {
  name        = local.kinesis_firehose_name
  destination = "extended_s3"

  # Source: read from Kinesis Data Stream (not direct PUT)
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.stock_market.arn
    role_arn           = aws_iam_role.firehose_delivery.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_delivery.arn
    bucket_arn = aws_s3_bucket.data_lake.arn

    # Partition data by time for easy Athena/Glue querying
    prefix              = local.firehose_prefix
    error_output_prefix = local.firehose_error_prefix

    buffering_size     = var.firehose_buffer_size_mb
    buffering_interval = var.firehose_buffer_interval_seconds
    compression_format = var.firehose_s3_compression

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose_errors.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3_delivery.name
    }
  }
}