# ─────────────────────────────────────────────
# CloudWatch Log Group (Firehose error logs)
# ─────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "firehose_errors" {
  name              = "/aws/kinesisfirehose/${local.generic_prefix}"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "firehose_s3_delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose_errors.name
}