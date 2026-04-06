# 📡 Stock Market Kinesis Pipeline

> **End-to-end real-time data streaming** with AWS Kinesis Data Stream, Kinesis Firehose, S3, and a Python producer that pulls live stock quotes via Yahoo Finance — all provisioned with Terraform.

---

## Table of Contents

1. [What is AWS Kinesis?](#1-what-is-aws-kinesis)
2. [Architecture Overview](#2-architecture-overview)
3. [Key Concepts Deep-Dive](#3-key-concepts-deep-dive)
4. [Project Structure](#4-project-structure)
5. [Prerequisites](#5-prerequisites)
6. [Deploy the Infrastructure](#6-deploy-the-infrastructure)
7. [Run the Producer](#7-run-the-producer)
8. [Query Your Data in S3](#8-query-your-data-in-s3)
9. [Monitoring & Alarms](#9-monitoring--alarms)
10. [Teardown](#10-teardown)
11. [Troubleshooting](#11-troubleshooting)
12. [Cost Estimate](#12-cost-estimate)

---

## 1. What is AWS Kinesis?

**Amazon Kinesis** is a family of services for collecting, processing, and analyzing real-time streaming data at scale. Think of it as a *highway for data* — instead of storing data first and processing it later (batch), Kinesis lets you react to data the moment it arrives.

### The Kinesis Family

| Service | What it does | When to use it |
|---|---|---|
| **Kinesis Data Streams** | Durable, low-latency real-time ingest layer. Your producers PUT records; your consumers GET records. | When you need custom consumers (Lambda, KCL, Flink) or multiple consumers reading the same data. |
| **Kinesis Data Firehose** | Fully managed delivery pipeline. Reads from a stream (or accepts direct PUTs) and delivers to S3, Redshift, OpenSearch, Splunk, etc. | When you just need data in a destination without writing consumer code. |
| **Kinesis Data Analytics** | Run SQL or Apache Flink on streaming data in real time. | When you need aggregations, windowed joins, or anomaly detection on live data. |
| **Kinesis Video Streams** | Ingest and store video from cameras and devices. | IoT / surveillance / ML video pipelines. |

### This Project Uses

```
Kinesis Data Stream  +  Kinesis Data Firehose  →  S3
```

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                          PRODUCER (local / EC2 / ECS)               │
│                                                                     │
│   yfinance ──► StockFetcher ──► KinesisProducer ──► PutRecords API  │
└────────────────────────────────────┬────────────────────────────────┘
                                     │  JSON records, partition key = ticker
                                     ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                                   AWS                                      │
│                                                                            │
│  ┌─────────────────────┐     ┌──────────────────────┐     ┌────────────┐  │
│  │  Kinesis Data       │────►│  Kinesis Firehose    │────►│   S3       │  │
│  │  Stream             │     │  Delivery Stream     │     │  Data Lake │  │
│  │  1 shard · 24h ret  │     │  5 MB / 60s buffer   │     │  GZIP      │  │
│  │  SSE (KMS)          │     │  GZIP · error logs   │     │  Partitioned│  │
│  └─────────────────────┘     └──────────────────────┘     └────────────┘  │
│                                        │                                   │
│                                        ▼                                   │
│                              ┌──────────────────┐                         │
│                              │   CloudWatch     │                         │
│                              │  Logs + Alarms   │                         │
│                              └──────────────────┘                         │
│                                                                            │
│  IAM: Producer User (PutRecords only) · Firehose Role (read stream + S3)  │
└────────────────────────────────────────────────────────────────────────────┘
```

Open `architecture.html` in your browser for a visual interactive diagram.

---

## 3. Key Concepts Deep-Dive

### 3.1 Kinesis Data Streams — How it works

A **Kinesis Data Stream** is composed of **shards**. Each shard is an ordered sequence of records.

```
Stream: "stock-market-kinesis-dev-stream"
│
├── Shard 0001  ──►  [AAPL record] [AAPL record] [MSFT record] ...
└── Shard 0002  ──►  [TSLA record] [GOOGL record] [AMZN record] ...
```

**Capacity per shard:**
- **Write**: 1 MB/s or 1,000 records/s
- **Read**: 2 MB/s (shared across all consumers)

**Partition Key** determines which shard a record lands on (via MD5 hash).
We use the **ticker symbol** as the partition key so all records for the same stock go to the same shard — preserving ordering per ticker.

**Record anatomy:**
```json
{
  "PartitionKey": "AAPL",
  "Data": "<base64-encoded JSON payload>",
  "SequenceNumber": "4959357244509892940983494034"
}
```

### 3.2 PutRecords vs PutRecord

| | `PutRecord` | `PutRecords` |
|---|---|---|
| Records per call | 1 | Up to 500 |
| Max payload | 1 MB | 5 MB total |
| Ordering guarantee | Per-shard ordered | Best-effort (failed records lose order) |
| Use when | You need strict ordering | You need throughput |

This producer uses **PutRecords** for efficiency and retries failed records individually.

### 3.3 Firehose Buffering

Firehose does not write every record to S3 immediately — it **buffers** records and writes in larger chunks:

```
Records arrive ──► [buffer] ──► flush when:
                                  • size >= 5 MB  (configurable 1–128 MB)
                                  • OR time >= 60 s (configurable 60–900 s)
                               ──► write .gz file to S3
```

This makes S3 storage efficient (fewer, larger files) while the stream itself remains low-latency.

### 3.4 S3 Partitioning

Firehose writes data to a Hive-compatible partition layout:

```
s3://your-bucket/stock-data/
  year=2026/
    month=04/
      day=02/
        hour=14/
          stock-market-kinesis-dev-firehose-1-2026-04-02-14-00-00.gz
```

This layout is natively understood by **AWS Athena** and **AWS Glue**, letting you query data like:

```sql
SELECT ticker, AVG(current_price), MAX(volume)
FROM stock_market_db.quotes
WHERE year='2026' AND month='04' AND day='02'
GROUP BY ticker
ORDER BY 2 DESC;
```

### 3.5 Encryption at Rest

- **Kinesis stream**: encrypted with AWS-managed KMS key (`alias/aws/kinesis`).
- **S3 bucket**: encrypted with AES-256 (SSE-S3).
- **In transit**: all AWS SDK calls use TLS 1.2+.

### 3.6 IAM Least Privilege

```
Producer (IAM User)           Firehose (IAM Role)
──────────────────            ───────────────────
kinesis:PutRecord     ✓       kinesis:GetRecords    ✓
kinesis:PutRecords    ✓       kinesis:GetShardIterator ✓
kinesis:DescribeStream ✓      kinesis:DescribeStream ✓
kinesis:ListShards    ✓       s3:PutObject          ✓
kms:GenerateDataKey   ✓       kms:Decrypt           ✓
kms:Decrypt           ✓       logs:PutLogEvents     ✓
```

No producer credential ever touches S3. No Firehose role can PUT to Kinesis.

---

## 4. Project Structure

```
AWS Kinesis/
│
├── terraform/
│   ├── providers.tf      # Terraform + AWS provider config
│   ├── variables.tf      # All input variables with descriptions
│   ├── main.tf           # All AWS resources (S3, Kinesis, Firehose, IAM, CW)
│   └── outputs.tf        # Useful values after apply (stream name, bucket, etc.)
│
├── producer/
│   ├── producer.py       # Main producer script
│   ├── requirements.txt  # Python dependencies
│   └── .env.example      # Environment variable template
│
├── architecture.html     # Interactive architecture diagram
└── README.md             # This file
```

---

## 5. Prerequisites

### Tools
- **Terraform** >= 1.5 — [install](https://developer.hashicorp.com/terraform/install)
- **Python** >= 3.10 — [install](https://python.org/downloads)
- **AWS CLI** >= 2.0 — [install](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

### AWS Account
You need an AWS account with permissions to create:
- Kinesis Data Streams
- Kinesis Firehose Delivery Streams
- S3 Buckets
- IAM Users, Roles, Policies
- CloudWatch Log Groups, Alarms

### AWS Credentials for Terraform
Configure your AWS credentials before running Terraform:

```bash
# Option A – Named profile (recommended)
aws configure --profile kinesis-project
export AWS_PROFILE=kinesis-project

# Option B – Environment variables
export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
```

---

## 6. Deploy the Infrastructure

### Step 1 — Initialize Terraform

```bash
cd terraform/
terraform init
```

Expected output:
```
Initializing provider plugins...
- hashicorp/aws v5.x.x
- hashicorp/random v3.x.x
✓ Terraform has been successfully initialized!
```

### Step 2 — Review the plan

```bash
terraform plan
```

You should see ~12 resources to be created. Review them — no changes are made yet.

### Step 3 — Apply

```bash
terraform apply
```

Type `yes` when prompted. Provisioning takes about 1–2 minutes.

### Step 4 — Note your outputs

```bash
terraform output
terraform output -raw producer_secret_access_key   # store this securely!
```

Key outputs:
```
kinesis_stream_name          = "stock-market-kinesis-dev-stream"
s3_bucket_name               = "stock-market-kinesis-dev-datalake-a1b2c3d4"
producer_access_key_id       = "AKIA..."
producer_env_snippet         = <<heredoc ... heredoc
```

### Customize (optional)

Edit `terraform.tfvars` (create it) to override defaults:

```hcl
# terraform/terraform.tfvars
project_name            = "my-stocks"
environment             = "dev"
aws_region              = "eu-west-1"
kinesis_shard_count     = 2
kinesis_retention_hours = 48
firehose_buffer_size_mb = 10
```

---

## 7. Run the Producer

### Step 1 — Set up Python environment

```bash
cd producer/

# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

pip install -r requirements.txt
```

### Step 2 — Configure credentials

```bash
cp .env.example .env
```

Edit `.env` and fill in the values from `terraform output`:

```env
AWS_REGION=us-east-1
KINESIS_STREAM_NAME=stock-market-kinesis-dev-stream
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=<your-secret>
```

### Step 3 — Run it!

```bash
# Default: AAPL MSFT GOOGL AMZN TSLA, every 10 seconds, forever
python producer.py

# Custom tickers, 5-second interval, run for 5 minutes
python producer.py --tickers AAPL NVDA AMD META --interval 5 --duration 300

# Enable debug logging
python producer.py --debug

# Show help
python producer.py --help
```

**Sample output:**

```
2026-04-02T14:30:00 [INFO] ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
2026-04-02T14:30:00 [INFO]   Stock Market Kinesis Producer
2026-04-02T14:30:00 [INFO]   Stream  : stock-market-kinesis-dev-stream
2026-04-02T14:30:00 [INFO]   Tickers : AAPL, MSFT, GOOGL, AMZN, TSLA
2026-04-02T14:30:00 [INFO]   Interval: 10 s
2026-04-02T14:30:00 [INFO] ✓ Connected to Kinesis stream  [status=ACTIVE, shards=1]

TICKER      PRICE       CHG     CHG%         VOLUME  COMPANY
───────────────────────────────────────────────────────────────────────────
AAPL      $225.50     +1.20   +0.54%    52,841,300  Apple Inc.
MSFT      $415.80     -0.90   -0.22%    18,203,100  Microsoft Corporation
GOOGL     $178.40     +2.30   +1.31%    24,500,000  Alphabet Inc.
AMZN      $195.20     +0.70   +0.36%    31,200,400  Amazon.com Inc.
TSLA      $248.90     -5.10   -2.01%    89,320,000  Tesla Inc.

2026-04-02T14:30:02 [INFO] Sent 5/5 records to Kinesis.
2026-04-02T14:30:02 [INFO] Sleeping 8.0 s until next poll…
```

### What the producer sends (JSON record example)

```json
{
  "event_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "event_time": "2026-04-02T14:30:01.234567+00:00",
  "ticker": "AAPL",
  "company_name": "Apple Inc.",
  "currency": "USD",
  "exchange": "NMS",
  "current_price": 225.50,
  "open_price": 224.30,
  "previous_close": 224.30,
  "day_high": 226.10,
  "day_low": 223.80,
  "fifty_two_week_high": 260.10,
  "fifty_two_week_low": 164.08,
  "volume": 52841300,
  "avg_volume": 58200000,
  "market_cap": 3420000000000,
  "price_change": 1.20,
  "price_change_pct": 0.5349,
  "day_range_pct": 1.0257,
  "pe_ratio": 32.4,
  "dividend_yield": 0.0051,
  "beta": 1.24,
  "eps": 6.95,
  "producer_version": "1.0.0",
  "data_source": "yfinance"
}
```

---

## 8. Query Your Data in S3

After data has been flowing for at least 60 seconds, you'll see GZIP files appear in S3.

### List the files

```bash
aws s3 ls s3://$(terraform output -raw s3_bucket_name)/stock-data/ --recursive
```

### Read a file locally

```bash
aws s3 cp s3://BUCKET/stock-data/year=2026/month=04/day=02/hour=14/FILE.gz - \
  | gunzip \
  | head -5 \
  | python -m json.tool
```

### Query with AWS Athena (recommended)

1. Go to **AWS Glue** → Crawlers → Create crawler pointing to your S3 prefix.
2. Run the crawler — it infers the JSON schema automatically.
3. Open **AWS Athena** and run:

```sql
SELECT
  ticker,
  COUNT(*)           AS quote_count,
  AVG(current_price) AS avg_price,
  MAX(day_high)      AS peak_high,
  MIN(day_low)       AS trough_low,
  SUM(volume)        AS total_volume
FROM stock_quotes
WHERE year = '2026' AND month = '04'
GROUP BY ticker
ORDER BY avg_price DESC;
```

---

## 9. Monitoring & Alarms

Two CloudWatch alarms are pre-configured:

| Alarm | Metric | Threshold | Meaning |
|---|---|---|---|
| `write-throttled` | `WriteProvisionedThroughputExceeded` | > 0 for 2 min | Producer is hitting shard limits → add shards |
| `firehose-freshness` | `DeliveryToS3.DataFreshness` | > 900 s for 6 min | Firehose is lagging → check S3 permissions |

**View alarms:**
```bash
aws cloudwatch describe-alarms --alarm-name-prefix "stock-market-kinesis-dev"
```

**View Firehose error logs:**
```bash
aws logs tail /aws/kinesisfirehose/stock-market-kinesis-dev --follow
```

---

## 10. Teardown

To destroy all AWS resources and stop incurring costs:

```bash
cd terraform/
terraform destroy
```

> ⚠️ This will permanently delete the S3 bucket and all data in it (because `force_destroy = true` in dev). Export any data you want to keep first.

---

## 11. Troubleshooting

**`UnauthorizedOperation` when running Terraform**
→ Your IAM user/role needs permissions to create the resources listed in Section 5.

**`ResourceNotFoundException` in producer**
→ Check `KINESIS_STREAM_NAME` in your `.env` matches `terraform output kinesis_stream_name`. Also ensure the stream is `ACTIVE` (wait ~30 s after apply).

**`ProvisionedThroughputExceededException` in producer**
→ You're sending more than 1 MB/s or 1,000 records/s per shard. The producer has built-in exponential backoff retry. For sustained high volume, increase `kinesis_shard_count` in `variables.tf`.

**No files appearing in S3 after 5 minutes**
→ Firehose needs at least 60 seconds to buffer. Check the Firehose error log group: `/aws/kinesisfirehose/stock-market-kinesis-dev`.

**yfinance returning `None` prices**
→ Markets may be closed (weekends/holidays). yfinance returns the last available price. The producer handles `None` values gracefully.

---

## 12. Cost Estimate

For a dev setup running 8 hours/day:

| Service | Usage | Est. monthly cost |
|---|---|---|
| Kinesis Data Stream | 1 shard | ~$11 |
| Kinesis Firehose | 5 tickers × 10s × 1 KB ≈ 43 MB/day | < $1 |
| S3 Storage | ~1 GB/month (compressed) | < $1 |
| CloudWatch | Logs + 2 alarms | < $1 |
| **Total** | | **~$13/month** |

> Tip: Destroy the stack when not in use — `terraform destroy` takes 30 seconds and saves you money.

---

## Learning Resources

- [Amazon Kinesis Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/introduction.html)
- [Kinesis Data Firehose Developer Guide](https://docs.aws.amazon.com/firehose/latest/dev/what-is-this-service.html)
- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [yfinance Documentation](https://github.com/ranaroussi/yfinance)

---

*Project: stock-market-kinesis · Author: Raul Pontello · April 2026*