locals {
  generic_prefix        = "${var.project_name}-${var.environment}"
  bucket_name           = "${local.generic_prefix}-s3-bucket"
  kinesis_stream_name   = "${local.generic_prefix}-kinesis-stream"
  kinesis_firehose_name = "${local.generic_prefix}-kinesis-firehose"
  firehose_prefix       = "stock-data/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
  firehose_error_prefix = "errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"
}