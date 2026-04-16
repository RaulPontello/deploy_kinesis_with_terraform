# Stock Market Kinesis Pipeline

> This is an education project: End-to-end real-time data streaming with AWS Kinesis Data Stream, Kinesis Firehose, S3, Lambda Function and SNS for alerts and a Python producer that pulls live stock quotes via Yahoo Finance — all provisioned with Terraform.

---

## Table of Contents

1. [What is AWS Kinesis?](#1-what-is-aws-kinesis)
2. [Architecture Overview](#2-architecture-overview)
3. [Key Concepts](#3-key-concepts)
4. [Project Structure](#4-project-structure)
5. [Prerequisites](#5-prerequisites)
6. [Connect Terraform to AWS](#6-connect-terraform-to-aws)
7. [Deploy the Infrastructure](#7-deploy-the-infrastructure)
8. [Run the Producer](#8-run-the-producer)
9. [Query Data in S3](#9-query-data-in-s3)
10. [Spike Detection (Lambda + SNS)](#10-spike-detection-lambda--sns)
11. [Monitoring](#11-monitoring)
12. [Teardown](#12-teardown)
13. [Troubleshooting](#13-troubleshooting)
14. [Official Documentation](#14-official-documentation)

---

## 1. What is AWS Kinesis?

**Amazon Kinesis** is a family of services for collecting, processing, and analyzing real-time streaming data at scale.

| Service | What it does | When to use it |
|---|---|---|
| **Kinesis Data Streams** | Durable, low-latency real-time ingest layer. Producers PUT records; consumers GET records. | When you need custom consumers (Lambda, KCL, Flink) or multiple consumers reading the same data. |
| **Kinesis Data Firehose** | Fully managed delivery pipeline. Reads from a stream and delivers to S3, Redshift, OpenSearch, Splunk, etc. | When you just need data in a destination without writing consumer code. |
| **Kinesis Data Analytics** | Run SQL or Apache Flink on streaming data in real time. | Aggregations, windowed joins, or anomaly detection on live data. |
| **Kinesis Video Streams** | Ingest and store video from cameras and devices. | IoT, surveillance, or ML video pipelines. |

### This project uses

```
Python Producer (yfinance)
        │
        ▼
Kinesis Data Stream   (real-time ingestion, 1 shard, KMS encrypted)
        │
        ├──► Kinesis Data Firehose (buffering 5 MB / 60 s, GZIP compression)
        │           │
        │           ▼
        │    S3 Data Lake (partitioned by year / month / day / hour)
        │
        └──► Lambda – Spike Detector (batch 10, TRIM_HORIZON)
                    │
                    ▼
             SNS Topic → Email alert (price_change_pct ≥ ±1%)
```

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        PRODUCER  (local machine)                         │
│                                                                          │
│   yfinance ──► StockFetcher ──► KinesisProducer ──► PutRecords (batch)   │
└──────────────────────────────────┬───────────────────────────────────────┘
                                   │  JSON records
                                   │  PartitionKey = ticker symbol
                                   ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                 AWS                                      │
│                                                                          │
│  ┌───────────────────┐                                                   │
│  │  Kinesis Data     │──────────────────────────────────────────────┐    │
│  │  Stream           │                                              │    │
│  │  1 shard · 24h    │                                              │    │
│  │  SSE (KMS)        │                                              │    │
│  └─────────┬─────────┘                                              │    │
│            │                                                        │    │
│            ▼                                                        ▼    │
│  ┌──────────────────────┐   ┌──────────┐   ┌──────────────────────────┐  │
│  │  Kinesis Firehose    │──►│  S3      │   │  Lambda                  │  │
│  │  Delivery Stream     │   │  Data    │   │  Spike Detector          │  │
│  │  5 MB / 60s buffer   │   │  Lake    │   │  batch=10 · timeout=60s  │  │
│  │  GZIP · error logs   │   │  (GZIP)  │   └──────────┬───────────────┘  │
│  └──────────┬───────────┘   └──────────┘              │                  │
│             │                                         ▼                  │
│             ▼                               ┌──────────────────────┐     │
│  ┌──────────────────┐                       │  SNS Topic           │     │
│  │   CloudWatch     │                       │  spike-alerts        │     │
│  │   Logs           │                       │         │            │     │
│  └──────────────────┘                       └─────────┼────────────┘     │
│                                                       ▼                  │
│                                               Email alert                │
│                                                                          │
│  IAM: Producer User       (PutRecords only)                              │
│       Firehose Role       (read stream + write S3)                       │
│       Lambda Exec Role    (read stream + SNS publish + CW logs)          │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Key Concepts

### Kinesis Data Streams — Shards

A stream is composed of **shards**. Each shard is an ordered, independent sequence of records.

**Capacity per shard:**
- Write: 1 MB/s or 1,000 records/s
- Read: 2 MB/s (shared across all consumers)

**Partition key** determines which shard a record lands on (via MD5 hash). This project uses the **ticker symbol** so all records for the same stock go to the same shard, preserving per-ticker ordering.

### PutRecord vs PutRecords

| | `PutRecord` | `PutRecords` |
|---|---|---|
| Records per call | 1 | Up to 500 |
| Max payload | 1 MB | 5 MB total |
| Ordering | Guaranteed per shard | Best-effort (failed records may lose order) |

This producer uses **PutRecords** for throughput efficiency and retries failed records with exponential backoff.

### Firehose Buffering

Firehose buffers records before writing to S3:

```
Records arrive ──► [buffer] ──► flush when:
                                 • size  >= 5 MB  (configurable: 1–128 MB)
                                 • OR time >= 60 s (configurable: 60–900 s)
                              ──► write .gz file to S3
```

### S3 Partitioning

Firehose writes to a Hive-compatible layout, natively understood by Athena and Glue:

```
s3://your-bucket/stock-data/
  year=2026/
    month=04/
      day=08/
        hour=14/
          stock-market-kinesis-dev-firehose-1-2026-04-08-14-00-00.gz
```

### IAM Least Privilege

```
Producer IAM User          Firehose IAM Role          Lambda Execution Role
─────────────────          ──────────────────         ─────────────────────────
kinesis:PutRecord          kinesis:GetRecords          kinesis:GetRecords
kinesis:PutRecords         kinesis:GetShardIterator    kinesis:GetShardIterator
kinesis:DescribeStream     kinesis:DescribeStream      kinesis:DescribeStream
kinesis:ListShards         s3:PutObject                kinesis:ListShards
kms:GenerateDataKey        kms:Decrypt                 kms:Decrypt
kms:Decrypt                logs:PutLogEvents           logs:CreateLogGroup
                                                       logs:PutLogEvents
                                                       sns:Publish
```

No producer credential ever touches S3. No Firehose role can write to Kinesis. The Lambda role cannot write records back to the stream.

---

## 4. Project Structure

```
deploy_kinesis_with_terraform/
│
├── .gitignore                    # Excludes .env, state files, .terraform/
├── README.md
│
├── terraform/                    # All Terraform configuration
│   ├── providers.tf              # Terraform + AWS provider requirements
│   ├── variables.tf              # All input variables with descriptions and defaults
│   ├── locals.tf                 # Computed local values (names, prefixes)
│   ├── kinesis.tf                # Kinesis Data Stream + Firehose resources
│   ├── s3.tf                     # S3 bucket and public access block
│   ├── iam.tf                    # IAM roles, users, and policies (Firehose + Producer + Lambda)
│   ├── lambda.tf                 # Lambda function + Kinesis event source mapping
│   ├── sns.tf                    # SNS topic + email subscription for spike alerts
│   ├── cloud_watch.tf            # CloudWatch log group and stream
│   └── outputs.tf                # Outputs after apply (stream name, bucket, keys, Lambda, SNS)
│
├── lambda/                       # Lambda function source
│   └── spike_detector.py         # Spike detection handler (no extra dependencies)
│
└── data_producer/                # Python producer application
    ├── stock_producer.py         # Main producer script
    ├── requirements.txt          # Python dependencies
    └── .env.example              # Environment variable template (no real credentials)
```

---

## 5. Prerequisites

### Tools

| Tool | Minimum version | Install |
|---|---|---|
| Terraform | >= 1.5 | [developer.hashicorp.com/terraform/install](https://developer.hashicorp.com/terraform/install) |
| Python | >= 3.10 | [python.org/downloads](https://www.python.org/downloads) |
| AWS CLI | >= 2.0 | [docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) |

Verify your installations:

```bash
terraform --version
python --version
aws --version
```

### AWS permissions

Your AWS identity (user or role) must have permissions to create:
- Kinesis Data Streams and Firehose Delivery Streams
- S3 Buckets and bucket policies
- IAM Users, Roles, and Policies
- CloudWatch Log Groups and Log Streams

---

## 6. Connect Terraform to AWS

Terraform needs AWS credentials to create and manage resources. There are two recommended approaches.

### Option A — Named AWS profile (recommended)

A **named profile** stores your credentials in `~/.aws/credentials` under a profile name. This keeps credentials out of environment variables and out of your code.

**Step 1: Create an IAM user** in the [AWS IAM Console](https://console.aws.amazon.com/iam/) with programmatic access and attach the permissions required by this project (or `AdministratorAccess` for development).

**Step 2: Configure the profile**

```bash
aws configure --profile my-profile
```

You will be prompted for:
```
AWS Access Key ID [None]:     AKIA...
AWS Secret Access Key [None]: your-secret-key
Default region name [None]:   us-east-1
Default output format [None]: json
```

The credentials are saved to the AWS credentials file on your machine:

| OS | File location |
|---|---|
| macOS / Linux | `~/.aws/credentials` |
| Windows | `C:\Users\YOUR_USERNAME\.aws\credentials` |

**Step 3: Tell Terraform which profile to use**

The `profile_name` variable in [terraform/variables.tf](terraform/variables.tf) is passed to the AWS provider. Set it to match your profile name.

You can override it at plan/apply time:

```bash
terraform plan -var="profile_name=my-profile"
```

Or create a `terraform/terraform.tfvars` file (never commit this file):

```hcl
profile_name = "my-profile"
```

**Verify your credentials work:**

```bash
aws sts get-caller-identity --profile my-profile
```

---

### Option B — Environment variables

If you prefer not to use a named profile, set credentials as environment variables in your terminal session.

**macOS / Linux** (bash or zsh):
```bash
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```
To persist across sessions, add those lines to `~/.bashrc` or `~/.zshrc`.

**Windows — Command Prompt:**
```cmd
set AWS_ACCESS_KEY_ID=AKIA...
set AWS_SECRET_ACCESS_KEY=your-secret-key
set AWS_DEFAULT_REGION=us-east-1
```

**Windows — PowerShell:**
```powershell
$env:AWS_ACCESS_KEY_ID="AKIA..."
$env:AWS_SECRET_ACCESS_KEY="your-secret-key"
$env:AWS_DEFAULT_REGION="us-east-1"
```
To persist across PowerShell sessions, use `[System.Environment]::SetEnvironmentVariable(...)` or set them in **System Properties → Environment Variables**.

When environment variables are set, the AWS provider picks them up automatically and the `profile_name` variable is ignored.

---

## 7. Deploy the Infrastructure

All Terraform commands must be run from the `terraform/` directory.

```bash
cd terraform/
```

### Step 1 — `terraform init`

Downloads the AWS provider plugin and prepares the working directory.

```bash
terraform init
```

Expected output:
```
Initializing the backend...
Initializing provider plugins...
- Finding hashicorp/aws versions matching "~> 6.0"...
- Installing hashicorp/aws v6.x.x...
Terraform has been successfully initialized!
```

Run this once after cloning the repo, and again whenever you add or upgrade a provider.

---

### Step 2 — `terraform validate`

Checks the configuration for syntax errors and internal consistency. Makes no AWS API calls.

```bash
terraform validate
```

Expected output:
```
Success! The configuration is valid.
```

---

### Step 3 — `terraform plan -out my_plan.plan`

Compares your configuration against the current state of AWS and shows exactly what will be created, updated, or destroyed. The `-out` flag saves the plan to a file so that the apply step executes exactly what was reviewed.

```bash
terraform plan -out my_plan.plan
```

Expected output (first deploy, ~17 resources):
```
Plan: 17 to add, 0 to change, 0 to destroy.

─────────────────────────────────────────────────────────────
Saved the plan to: my_plan.plan
```

#### `terraform show my_plan.plan` — read the saved plan

The plan file is binary. Use `terraform show` to render it as human-readable text before applying:

```bash
terraform show my_plan.plan
```

This prints every resource action (`+` create, `~` update, `-` destroy) with all attribute values, making it easy to review the exact changes Terraform will make:

```
# aws_kinesis_stream.stock_market will be created
+ resource "aws_kinesis_stream" "stock_market" {
    + arn              = (known after apply)
    + name             = "stock-market-kinesis-dev-kinesis-stream"
    + shard_count      = 1
    ...
  }

# aws_lambda_function.spike_detector will be created
+ resource "aws_lambda_function" "spike_detector" {
    + function_name = "stock-market-kinesis-dev-spike-detector"
    + runtime       = "python3.12"
    ...
  }
```

Review the plan carefully before proceeding. No changes are made at this step.

---

### Step 4 — `terraform apply my_plan.plan`

Applies the saved plan and creates the AWS resources. Because the plan file is provided, no confirmation prompt is shown — exactly what was reviewed is applied.

```bash
terraform apply my_plan.plan
```

Provisioning takes about 1–2 minutes. Expected output:
```
aws_s3_bucket.data_lake: Creating...
aws_kinesis_stream.stock_market: Creating...
...
Apply complete! Resources: 12 added, 0 changed, 0 destroyed.
```

---

### Step 5 — `terraform output`

Displays the values produced after apply: stream name, bucket name, and IAM credentials for the producer.

```bash
terraform output
```

To retrieve the sensitive secret access key:

```bash
terraform output -raw producer_secret_access_key
```

To get a ready-to-paste `.env` block:

```bash
terraform output -raw producer_env_snippet
```

Key outputs:

| Output | Description |
|---|---|
| `kinesis_stream_name` | Stream name to set in `.env` |
| `s3_bucket_name` | Where Firehose delivers data |
| `producer_access_key_id` | AWS key ID for the producer IAM user |
| `producer_secret_access_key` | AWS secret key (sensitive) |
| `cloudwatch_log_group` | Firehose error log group |
| `sns_spike_alerts_arn` | ARN of the SNS spike-alert topic |
| `lambda_function_name` | Name of the spike detector Lambda |
| `lambda_function_arn` | ARN of the spike detector Lambda |

---

### Step 6 — `terraform show`

After applying, `terraform show` reads the state file and prints every managed resource with its current attribute values. Unlike `terraform output`, this shows the full picture — not just the declared outputs.

```bash
terraform show
```

Useful for confirming actual values that were only known after creation, such as ARNs, IDs, and generated names:

```
# aws_kinesis_stream.stock_market:
resource "aws_kinesis_stream" "stock_market" {
    arn              = "arn:aws:kinesis:us-east-1:123456789012:stream/stock-market-kinesis-dev-kinesis-stream"
    name             = "stock-market-kinesis-dev-kinesis-stream"
    shard_count      = 1
    stream_mode_details {
        stream_mode = "PROVISIONED"
    }
}

# aws_lambda_function.spike_detector:
resource "aws_lambda_function" "spike_detector" {
    arn           = "arn:aws:lambda:us-east-1:123456789012:function:stock-market-kinesis-dev-spike-detector"
    function_name = "stock-market-kinesis-dev-spike-detector"
    runtime       = "python3.12"
}
```

---

### Step 7 — `terraform state list`

Lists every resource currently tracked in the Terraform state file. Use this to get a quick inventory of what is deployed without reading through all attributes.

```bash
terraform state list
```

Expected output:
```
data.archive_file.spike_detector
data.aws_iam_policy_document.firehose_assume_role
data.aws_iam_policy_document.firehose_permissions
data.aws_iam_policy_document.lambda_assume_role
data.aws_iam_policy_document.lambda_permissions
data.aws_iam_policy_document.producer_permissions
aws_cloudwatch_log_group.firehose_errors
aws_cloudwatch_log_stream.firehose_s3_delivery
aws_iam_access_key.producer
aws_iam_role.firehose_delivery
aws_iam_role.lambda_spike_detector
aws_iam_role_policy.firehose_delivery
aws_iam_role_policy.lambda_spike_detector
aws_iam_user.producer
aws_iam_user_policy.producer
aws_kinesis_firehose_delivery_stream.to_s3
aws_kinesis_stream.stock_market
aws_lambda_event_source_mapping.kinesis_to_lambda
aws_lambda_function.spike_detector
aws_s3_bucket.data_lake
aws_s3_bucket_public_access_block.data_lake
aws_sns_topic.spike_alerts
aws_sns_topic_subscription.alert_email
```

To inspect a single resource from the list:

```bash
terraform state show aws_lambda_function.spike_detector
terraform state show aws_sns_topic.spike_alerts
```

---

### Optional: customize variables

Create `terraform/terraform.tfvars` to override defaults without editing `variables.tf`:

```hcl
# terraform/terraform.tfvars  (do not commit this file)
project_name                     = "stock-market-kinesis"
environment                      = "dev"
aws_region                       = "us-east-1"
kinesis_shard_count              = 1
kinesis_retention_hours          = 24
firehose_buffer_size_mb          = 5
firehose_buffer_interval_seconds = 60
firehose_s3_compression          = "GZIP"

alert_email           = "you@example.com"  # default is set; override to your address
anomaly_threshold_pct = 1                  # default is 1%
```

> **SNS email confirmation:** After `terraform apply`, AWS sends a confirmation email to `alert_email`. You must click **"Confirm subscription"** in that email before any alerts will be delivered.

---

## 8. Run the Producer

### Step 1 — Create a Python virtual environment

A virtual environment isolates this project's dependencies from other Python projects on your machine.

```bash
cd data_producer/

# Create the virtual environment
python -m venv .venv

# Activate it
source .venv/bin/activate       # macOS / Linux
# .venv\Scripts\activate        # Windows (Command Prompt)
# .venv\Scripts\Activate.ps1    # Windows (PowerShell)
```

You will see `(.venv)` at the start of your terminal prompt when the environment is active.

To deactivate when done:

```bash
deactivate
```

---

### Step 2 — Install dependencies from requirements.txt

`requirements.txt` lists the exact Python packages the producer needs. Install them into the active virtual environment:

```bash
pip install -r requirements.txt
```

This installs:
- `boto3` — AWS SDK for Python (Kinesis API calls)
- `yfinance` — Yahoo Finance data fetching
- `python-dotenv` — loads `.env` file into environment variables

---

### Step 3 — Configure credentials with .env

`.env.example` is a template that shows which environment variables the producer needs. It contains no real credentials and is safe to commit.

Copy it and fill in the values from `terraform output`:

```bash
cp .env.example .env
```

Edit `.env`:

```env
AWS_REGION=us-east-1
KINESIS_STREAM_NAME=stock-market-kinesis-dev-kinesis-stream
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=your-secret-key
```

**The `.env` file must never be committed.** It is listed in `.gitignore`.

---

### Step 4 — Run the producer

```bash
# Default: AAPL MSFT GOOGL AMZN TSLA, every 10 seconds, runs forever
python stock_producer.py

# Custom tickers, 5-second interval, stop after 5 minutes
python stock_producer.py --tickers AAPL NVDA AMD META --interval 5 --duration 300

# Enable debug logging
python stock_producer.py --debug

# Show all options
python stock_producer.py --help
```

**Available arguments:**

| Argument | Default | Description |
|---|---|---|
| `--tickers` | AAPL MSFT GOOGL AMZN TSLA | Space-separated list of ticker symbols |
| `--stream` | value from `KINESIS_STREAM_NAME` env var | Override the stream name |
| `--interval` | `10` | Seconds between each fetch/send cycle |
| `--duration` | (none — runs forever) | Stop after this many seconds |
| `--debug` | off | Enable verbose debug logging |

**Sample output:**

```
2026-04-08T14:30:00 [INFO] stock-producer – Stream=stock-market-kinesis-dev-kinesis-stream  Tickers=AAPL,MSFT,GOOGL,AMZN,TSLA  Interval=10s
2026-04-08T14:30:00 [INFO] stock-producer – Connected: status=ACTIVE, shards=1
2026-04-08T14:30:02 [INFO] stock-producer – Sent 5/5 records → stock-market-kinesis-dev-kinesis-stream
2026-04-08T14:30:12 [INFO] stock-producer – Sent 5/5 records → stock-market-kinesis-dev-kinesis-stream
```

### What each record looks like (JSON)

```json
{
  "event_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "event_time": "2026-04-08T14:30:01.234567+00:00",
  "ticker": "AAPL",
  "current_price": 225.50,
  "price_change_pct": 0.5349
}
```

---

## 9. Query Data in S3

After the producer has been running for at least 60 seconds, GZIP files will appear in S3.

### List files

```bash
aws s3 ls s3://$(cd terraform && terraform output -raw s3_bucket_name)/stock-data/ --recursive
```

### Read a file locally

```bash
aws s3 cp s3://YOUR-BUCKET/stock-data/year=2026/month=04/day=08/hour=14/FILE.gz - \
  | gunzip \
  | head -5 \
  | python -m json.tool
```

### Query with AWS Athena

1. Go to **AWS Glue** → Crawlers → create a crawler pointing to your S3 prefix.
2. Run the crawler — it infers the JSON schema automatically.
3. Open **AWS Athena** and run:

```sql
SELECT
  ticker,
  COUNT(*)           AS quote_count,
  AVG(current_price) AS avg_price,
  MAX(day_high)      AS peak,
  MIN(day_low)       AS trough
FROM stock_quotes
WHERE year = '2026' AND month = '04'
GROUP BY ticker
ORDER BY avg_price DESC;
```

---

## 10. Spike Detection (Lambda + SNS)

A Lambda function (`spike_detector`) is triggered by the Kinesis Data Stream in real time. For every batch of up to 10 records it checks whether `abs(price_change_pct) >= anomaly_threshold_pct` and publishes an SNS alert for each anomaly it finds.

### How it works

```
Kinesis record batch (≤10)
        │
        ▼
spike_detector.handler()
  for each record:
    decode base64 → parse JSON
    if abs(price_change_pct) >= threshold:
        sns.publish(subject, message)
        log anomaly
        │
        ▼
  SNS Topic ──► Email
```

### Alert email format

```
Subject: [Stock Spike UP] AAPL moved +3.72%

SPIKE ALERT
===========
Direction      : UP
Ticker         : AAPL
Current Price  : $213.49
Change         : +3.7200%
Threshold      : ±1.0%
Time           : 2026-04-14T18:30:01.123456+00:00
```

### Tuning the threshold

Change `anomaly_threshold_pct` in `terraform.tfvars` and re-apply:

```hcl
anomaly_threshold_pct = 3.0   # only alert on moves ≥ 3%
```

```bash
terraform apply -var="anomaly_threshold_pct=3.0"
```

### Viewing Lambda logs

```bash
aws logs tail /aws/lambda/stock-market-kinesis-dev-spike-detector --follow
```

---

## 11. Monitoring

### CloudWatch Logs

Firehose delivery errors are written to:

```
/aws/kinesisfirehose/stock-market-kinesis-dev-kinesis-firehose
```

Follow logs in real time:

```bash
aws logs tail /aws/kinesisfirehose/stock-market-kinesis-dev-kinesis-firehose --follow
```

### Common Firehose metrics to watch

| Metric | What it means |
|---|---|
| `DeliveryToS3.Success` | Successful S3 deliveries |
| `DeliveryToS3.DataFreshness` | Age of oldest record in buffer (> 900 s = concern) |
| `WriteProvisionedThroughputExceeded` | Stream shard is saturated → add shards |

---

## 12. Teardown

To destroy all AWS resources and stop incurring costs:

```bash
cd terraform/
terraform destroy
```

Type `yes` when prompted.

> **Warning:** This permanently deletes the S3 bucket and all data in it (`force_destroy = true` is set for dev). Export any data you need before running this.

---

## 13. Troubleshooting

**`UnauthorizedOperation` when running Terraform**
Your IAM identity needs permissions to create the resources listed in Section 5. Check your profile or credentials are active.

**`ResourceNotFoundException` in the producer**
`KINESIS_STREAM_NAME` in `.env` does not match the deployed stream. Run `terraform output kinesis_stream_name` and update `.env`. Also ensure the stream status is `ACTIVE` (wait ~30 s after apply).

**`ProvisionedThroughputExceededException` in the producer**
You are sending more than 1 MB/s or 1,000 records/s per shard. The producer retries automatically with exponential backoff. For sustained high volume, increase `kinesis_shard_count` in `terraform.tfvars`.

**No files appearing in S3 after 5 minutes**
Firehose buffers for at least 60 seconds. Check the Firehose error log group for delivery failures.

**yfinance returns `None` prices**
Markets may be closed (weekends/holidays). yfinance returns the last available price. The producer handles `None` values gracefully — records are still sent, just with `null` fields.

**No spike alert emails received**
1. Check you clicked **"Confirm subscription"** in the AWS confirmation email sent to `alert_email`.
2. Verify the Lambda is enabled: AWS Console → Lambda → `*-spike-detector` → Triggers.
3. Check Lambda logs: `aws logs tail /aws/lambda/stock-market-kinesis-dev-spike-detector --follow`
4. The default threshold is ±1%. If `price_change_pct` is always `null` (markets closed), no alerts fire.

**Lambda event source mapping shows `disabled`**
This can happen if the Lambda encounters repeated errors. Check CloudWatch Logs for the function, fix the issue, then re-enable the trigger in the AWS Console or via `terraform apply`.

---

## 14. Official Documentation

### AWS

- [Amazon Kinesis Data Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/introduction.html)
- [Amazon Kinesis Data Firehose Developer Guide](https://docs.aws.amazon.com/firehose/latest/dev/what-is-this-service.html)
- [Amazon S3 User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
- [AWS IAM User Guide](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html)
- [AWS CLI Configuration Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html)
- [Amazon CloudWatch Logs User Guide](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/WhatIsCloudWatchLogs.html)
- [AWS Lambda Developer Guide](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html)
- [Using Lambda with Kinesis](https://docs.aws.amazon.com/lambda/latest/dg/with-kinesis.html)
- [Amazon SNS Developer Guide](https://docs.aws.amazon.com/sns/latest/dg/welcome.html)

### Terraform

- [Terraform AWS Provider — Kinesis Stream](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kinesis_stream)
- [Terraform AWS Provider — Kinesis Firehose](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/kinesis_firehose_delivery_stream)
- [Terraform AWS Provider — S3 Bucket](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket)
- [Terraform AWS Provider — IAM](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role)
- [Terraform AWS Provider — Lambda](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_function)
- [Terraform AWS Provider — SNS](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/sns_topic)
- [Terraform Language Documentation](https://developer.hashicorp.com/terraform/language)
- [Terraform CLI Commands](https://developer.hashicorp.com/terraform/cli/commands)

### Python libraries

- [boto3 — AWS SDK for Python](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [yfinance](https://github.com/ranaroussi/yfinance)
- [python-dotenv](https://github.com/theskumar/python-dotenv)

---
