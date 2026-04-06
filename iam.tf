
# ─────────────────────────────────────────────
# IAM Role - Firehose
# ─────────────────────────────────────────────

# Trust policy – only the Firehose service can assume this role
data "aws_iam_policy_document" "firehose_assume_role" {
  statement {
    sid    = "AllowFirehoseAssumeRole"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "firehose_delivery" {
  name               = "${local.kinesis_firehose_name}-iam-role"
  description        = "Allows Kinesis Firehose to read from the stream and deliver to S3"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume_role.json
}

# Permission policy – what Firehose is allowed to do once it assumes the role
data "aws_iam_policy_document" "firehose_permissions" {

  # 1. Read records from Kinesis Data Stream
  statement {
    sid    = "ReadFromKinesisStream"
    effect = "Allow"

    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetShardIterator",
      "kinesis:GetRecords",
      "kinesis:ListShards",
      "kinesis:SubscribeToShard",
    ]

    resources = [aws_kinesis_stream.stock_market.arn]
  }

  # 2. Write objects to the S3 data lake bucket
  statement {
    sid    = "WriteToS3DataLake"
    effect = "Allow"

    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]

    resources = [
      aws_s3_bucket.data_lake.arn,
      "${aws_s3_bucket.data_lake.arn}/*",
    ]
  }

  # 3. KMS – decrypt stream data and re-encrypt for S3
  #    Scoped to KMS calls made via Kinesis or S3 service endpoints only
  statement {
    sid    = "KMSDecryptAndEncrypt"
    effect = "Allow"

    actions = [
      "kms:GenerateDataKey",
      "kms:Decrypt",
    ]

    resources = ["*"]

    condition {
      test     = "StringLike"
      variable = "kms:ViaService"
      values = [
        "kinesis.${var.aws_region}.amazonaws.com",
        "s3.${var.aws_region}.amazonaws.com",
      ]
    }
  }

  # 4. CloudWatch Logs – push Firehose delivery error logs
  statement {
    sid    = "CloudWatchLogsDelivery"
    effect = "Allow"

    actions = [
      "logs:PutLogEvents",
      "logs:CreateLogStream",
    ]

    resources = [
      aws_cloudwatch_log_group.firehose_errors.arn,
      "${aws_cloudwatch_log_group.firehose_errors.arn}:*",
    ]
  }
}

# Inline policy attached directly to the Firehose role
resource "aws_iam_role_policy" "firehose_delivery" {
  name   = "${local.kinesis_firehose_name}-iam-policy"
  role   = aws_iam_role.firehose_delivery.id
  policy = data.aws_iam_policy_document.firehose_permissions.json
}