# ─────────────────────────────────────────────
#  Kinesis Data Stream
# ─────────────────────────────────────────────
output "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream (used in the producer script)"
  value       = aws_kinesis_stream.stock_market.name
}

output "kinesis_stream_arn" {
  description = "ARN of the Kinesis Data Stream"
  value       = aws_kinesis_stream.stock_market.arn
}

output "kinesis_stream_shard_count" {
  description = "Number of shards provisioned"
  value       = aws_kinesis_stream.stock_market.shard_count
}

# ─────────────────────────────────────────────
#  Kinesis Firehose
# ─────────────────────────────────────────────
output "firehose_delivery_stream_name" {
  description = "Name of the Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.to_s3.name
}

output "firehose_delivery_stream_arn" {
  description = "ARN of the Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.to_s3.arn
}

# ─────────────────────────────────────────────
#  S3 Data Lake
# ─────────────────────────────────────────────
output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.arn
}

output "s3_data_prefix" {
  description = "S3 prefix pattern where stock data is stored (partitioned by time)"
  value       = "s3://${aws_s3_bucket.data_lake.bucket}/stock-data/year=<YYYY>/month=<MM>/day=<DD>/hour=<HH>/"
}

# ─────────────────────────────────────────────
#  IAM – Firehose
# ─────────────────────────────────────────────
output "firehose_role_arn" {
  description = "ARN of the Firehose delivery IAM role"
  value       = aws_iam_role.firehose_delivery.arn
}

# ─────────────────────────────────────────────
#  CloudWatch
# ─────────────────────────────────────────────
output "cloudwatch_log_group" {
  description = "CloudWatch log group for Firehose error logs"
  value       = aws_cloudwatch_log_group.firehose_errors.name
}
