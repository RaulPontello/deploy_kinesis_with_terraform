locals {
  generic_prefix      = "${var.project_name}-${var.environment}"
  bucket_name         = "${local.generic_prefix}-s3-bucket"
  kinesis_stream_name = "${local.generic_prefix}-kinesis_stream"
}