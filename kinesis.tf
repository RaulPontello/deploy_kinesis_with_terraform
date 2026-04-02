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

  # Server-side encryption with AWS-managed KMS key
  dynamic "encryption_type" {
    for_each = var.kinesis_encryption_enabled ? [1] : []
    content {}
  }

  encryption_type = var.kinesis_encryption_enabled ? "KMS" : "NONE"
  kms_key_id      = var.kinesis_encryption_enabled ? "alias/aws/kinesis" : null

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# ─────────────────────────────────────────────
# Kinesis Data Firehose Delivery Stream
# ─────────────────────────────────────────────
