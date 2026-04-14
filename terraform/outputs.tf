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
#  IAM – Producer
# ─────────────────────────────────────────────
output "producer_iam_user" {
  description = "IAM username for the Kinesis producer"
  value       = aws_iam_user.producer.name
}

output "producer_access_key_id" {
  description = "AWS Access Key ID for the producer — paste into .env"
  value       = aws_iam_access_key.producer.id
  sensitive   = true
}

output "producer_secret_access_key" {
  description = "AWS Secret Access Key for the producer (sensitive)"
  value       = aws_iam_access_key.producer.secret
  sensitive   = true
}

output "producer_env_snippet" {
  description = "Ready-to-paste .env block — run: terraform output -raw producer_env_snippet"
  value       = <<-EOT
    AWS_REGION=${var.aws_region}
    KINESIS_STREAM_NAME=${aws_kinesis_stream.stock_market.name}
    AWS_ACCESS_KEY_ID=${aws_iam_access_key.producer.id}
    AWS_SECRET_ACCESS_KEY=<run: terraform output -raw producer_secret_access_key>
  EOT
  sensitive   = true
}

# ─────────────────────────────────────────────
#  CloudWatch
# ─────────────────────────────────────────────
output "cloudwatch_log_group" {
  description = "CloudWatch log group for Firehose error logs"
  value       = aws_cloudwatch_log_group.firehose_errors.name
}

# ─────────────────────────────────────────────
#  SNS
# ─────────────────────────────────────────────
output "sns_spike_alerts_arn" {
  description = "ARN of the SNS topic that receives spike alerts"
  value       = aws_sns_topic.spike_alerts.arn
}

# ─────────────────────────────────────────────
#  Lambda
# ─────────────────────────────────────────────
output "lambda_function_name" {
  description = "Name of the spike detector Lambda function"
  value       = aws_lambda_function.spike_detector.function_name
}

output "lambda_function_arn" {
  description = "ARN of the spike detector Lambda function"
  value       = aws_lambda_function.spike_detector.arn
}
