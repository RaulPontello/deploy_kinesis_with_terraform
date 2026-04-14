"""
Spike Detector Lambda
---------------------
Triggered by Kinesis Data Stream (event source mapping).

For each record batch:
  1. Base64-decode the Kinesis payload → JSON (StockQuote fields)
  2. Check abs(price_change_pct) >= ANOMALY_THRESHOLD_PCT
  3. If a spike is detected, publish an alert to SNS

Environment variables (set by Terraform):
  SNS_TOPIC_ARN         — ARN of the target SNS topic
  ANOMALY_THRESHOLD_PCT — percentage threshold (absolute); default 1.0
"""

import base64
import json
import logging
import os

import boto3

log = logging.getLogger()
log.setLevel(logging.INFO)

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
THRESHOLD_PCT = float(os.environ.get("ANOMALY_THRESHOLD_PCT", "1.0"))

sns = boto3.client("sns")


def handler(event, context):
    records = event.get("Records", [])
    log.info("Received batch of %d records", len(records))

    anomalies = 0

    for record in records:
        # Kinesis payloads are base64-encoded
        raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")

        try:
            quote = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Skipping unparseable record: %s", raw[:120])
            continue

        ticker           = quote.get("ticker", "UNKNOWN")
        price_change_pct = quote.get("price_change_pct")
        current_price    = quote.get("current_price")
        event_time       = quote.get("event_time", "")

        log.info(
            "ticker=%s  price=%s  change_pct=%s",
            ticker,
            current_price,
            price_change_pct,
        )

        if price_change_pct is None:
            log.warning("ticker=%s has no price_change_pct — skipping", ticker)
            continue

        if abs(price_change_pct) >= THRESHOLD_PCT:
            anomalies += 1
            direction = "UP" if price_change_pct > 0 else "DOWN"

            subject = f"[Stock Spike {direction}] {ticker} moved {price_change_pct:+.2f}%"
            message = (
                f"SPIKE ALERT\n"
                f"===========\n"
                f"Direction : {direction}\n"
                f"Ticker    : {ticker}\n"
                f"Price     : ${current_price}\n"
                f"Change    : {price_change_pct:+.4f}%\n"
                f"Threshold : ±{THRESHOLD_PCT}%\n"
                f"Time      : {event_time}\n"
            )

            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=message,
            )
            log.info("SNS alert published — ticker=%s change_pct=%.4f%%", ticker, price_change_pct)

    log.info("Batch complete — processed=%d anomalies=%d", len(records), anomalies)
    return {"processed": len(records), "anomalies": anomalies}
