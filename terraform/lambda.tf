# ─────────────────────────────────────────────
#  Lambda – Spike Detector
# ─────────────────────────────────────────────

# Package the Python handler into a zip Terraform can upload
data "archive_file" "spike_detector" {
  type        = "zip"
  source_file = "${path.module}/../lambda/spike_detector.py"
  output_path = "${path.module}/../lambda/spike_detector.zip"
}

# Lambda Function
resource "aws_lambda_function" "spike_detector" {
  function_name    = "${local.generic_prefix}-spike-detector"
  description      = "Detects price spikes from Kinesis records and publishes SNS alerts"
  filename         = data.archive_file.spike_detector.output_path
  source_code_hash = data.archive_file.spike_detector.output_base64sha256
  role             = aws_iam_role.lambda_spike_detector.arn
  handler          = "spike_detector.handler"
  runtime          = "python3.12"
  timeout          = 60

  environment {
    variables = {
      SNS_TOPIC_ARN         = aws_sns_topic.spike_alerts.arn
      ANOMALY_THRESHOLD_PCT = tostring(var.anomaly_threshold_pct)
    }
  }
}

# ─────────────────────────────────────────────
#  Event Source Mapping – Kinesis → Lambda
# ─────────────────────────────────────────────

resource "aws_lambda_event_source_mapping" "kinesis_to_lambda" {
  event_source_arn  = aws_kinesis_stream.stock_market.arn
  function_name     = aws_lambda_function.spike_detector.arn
  starting_position = "TRIM_HORIZON"
  batch_size        = 10
}
