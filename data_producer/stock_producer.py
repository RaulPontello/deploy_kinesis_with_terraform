""""
Streams real-time stock market data from Yahoo Finance (yfinance)
into an AWS Kinesis Data Stream.

Architecture
------------
  yfinance  ──►  StockFetcher  ──►  KinesisProducer  ──►  Kinesis Data Stream
                                          │
                                     (batch PUT)
                                          │
                                          ▼
                              CloudWatch metrics / local logs

Usage
-----
  # 1. Install dependencies
  pip install -r requirements.txt

  # 2. Configure credentials (choose one approach):
  #    a) Copy .env.example → .env and fill in the values
  #    b) Export environment variables
  #    c) Use an IAM role (on EC2/ECS/Lambda)

  # 3. Run
  python producer.py --tickers AAPL MSFT GOOGL --interval 5 --duration 300
"""

import argparse
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

import boto3
import yfinance as yf
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

DEFAULT_TICKERS          = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
DEFAULT_INTERVAL_SECONDS = 10
DEFAULT_DURATION_SECONDS = None  # None = run forever

AWS_REGION  = os.getenv("AWS_REGION", "us-east-1")
STREAM_NAME = os.getenv("KINESIS_STREAM_NAME", "stock-market-kinesis-dev-kinesis-stream")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("stock-producer")


@dataclass
class StockQuote:
    event_id:        str           = field(default_factory=lambda: str(uuid.uuid4()))
    event_time:      str           = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ticker:          str           = ""
    current_price:   float | None  = None
    open_price:      float | None  = None
    previous_close:  float | None  = None
    day_high:        float | None  = None
    day_low:         float | None  = None
    volume:          int   | None  = None
    market_cap:      int   | None  = None
    price_change:    float | None  = None
    price_change_pct: float | None = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


class StockFetcher:
    def __init__(self, tickers: list[str]) -> None:
        self.tickers = [t.upper() for t in tickers]

    def _safe_float(self, value) -> float | None:
        try:
            v = float(value)
            return v if v == v else None  # NaN check
        except (TypeError, ValueError):
            return None

    def _safe_int(self, value) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def fetch_quote(self, symbol: str) -> StockQuote | None:
        try:
            ticker = yf.Ticker(symbol)
            info   = ticker.fast_info

            # fast_info.last_price is served from yfinance's HTTP cache — use
            # history() instead, which always issues a fresh request to Yahoo Finance
            hist          = ticker.history(period="1d", interval="1m")
            current_price = self._safe_float(hist["Close"].iloc[-1]) if not hist.empty else None

            previous_close = self._safe_float(getattr(info, "previous_close", None))
            open_price     = self._safe_float(getattr(info, "open",           None))

            price_change     = None
            price_change_pct = None
            if current_price is not None and previous_close:
                price_change     = current_price - previous_close
                price_change_pct = (price_change / previous_close) * 100

            return StockQuote(
                ticker         = symbol,
                current_price  = current_price,
                open_price     = open_price,
                previous_close = previous_close,
                day_high       = self._safe_float(getattr(info, "day_high",  None)),
                day_low        = self._safe_float(getattr(info, "day_low",   None)),
                volume         = self._safe_int(getattr(info,  "three_month_average_volume", None)),
                market_cap     = self._safe_int(getattr(info,  "market_cap", None)),
                price_change     = price_change,
                price_change_pct = price_change_pct,
            )
        except Exception as exc:
            log.warning("Failed to fetch %s: %s", symbol, exc)
            return None

    def fetch_all(self) -> list[StockQuote]:
        return [q for ticker in self.tickers if (q := self.fetch_quote(ticker))]


class KinesisProducer:
    def __init__(self, stream_name: str, region: str = AWS_REGION, max_retries: int = 3) -> None:
        self.stream_name = stream_name
        self.max_retries = max_retries
        self.client      = boto3.client("kinesis", region_name=region)

    def put_records(self, quotes: list[StockQuote]) -> dict:
        if not quotes:
            return {"sent": 0, "failed": 0}

        records = [
            {"Data": q.to_json().encode(), "PartitionKey": q.ticker}
            for q in quotes
        ]

        remaining     = records
        cumulative_ok = 0

        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                sleep_time = 2 ** attempt
                log.warning("Retrying %d records (attempt %d, sleeping %ds)", len(remaining), attempt, sleep_time)
                time.sleep(sleep_time)

            try:
                response       = self.client.put_records(StreamName=self.stream_name, Records=remaining)
                failed_records = [
                    remaining[i]
                    for i, r in enumerate(response["Records"])
                    if "ErrorCode" in r
                ]
                cumulative_ok += len(remaining) - len(failed_records)
                remaining      = failed_records

                if not remaining:
                    break

            except (BotoCoreError, ClientError) as exc:
                log.error("Kinesis API error (attempt %d): %s", attempt, exc)

        return {"sent": cumulative_ok, "failed": len(remaining)}

    def describe_stream(self) -> dict:
        resp = self.client.describe_stream_summary(StreamName=self.stream_name)
        return resp["StreamDescriptionSummary"]


def run(tickers: list[str], stream_name: str, interval: int, duration: int | None) -> None:
    log.info("Stream=%s  Tickers=%s  Interval=%ds", stream_name, ",".join(tickers), interval)

    fetcher  = StockFetcher(tickers)
    producer = KinesisProducer(stream_name=stream_name)

    try:
        info = producer.describe_stream()
        log.info("Connected: status=%s, shards=%d", info.get("StreamStatus"), info.get("OpenShardCount", "?"))
    except Exception:
        log.error("Cannot connect to stream '%s'. Check credentials and stream name.", stream_name)
        sys.exit(1)

    start_time = time.time()

    try:
        while True:
            log.info("-----------------------------")
            
            cycle_start = time.time()
            quotes      = fetcher.fetch_all()

            if quotes:
                result = producer.put_records(quotes)
                log.info("Sent %d/%d records → %s", result["sent"], len(quotes), stream_name)
            else:
                log.warning("No quotes fetched.")

            if duration and time.time() - start_time >= duration:
                log.info("Duration of %ds reached. Stopping.", duration)
                break

            sleep_time = max(0.0, interval - (time.time() - cycle_start))
            if sleep_time:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info("Stopped by user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream stock quotes to AWS Kinesis")
    parser.add_argument("--tickers",  nargs="+", default=DEFAULT_TICKERS,          metavar="SYMBOL")
    parser.add_argument("--stream",   default=STREAM_NAME,                          metavar="NAME")
    parser.add_argument("--interval", type=int,  default=DEFAULT_INTERVAL_SECONDS,  metavar="SECONDS")
    parser.add_argument("--duration", type=int,  default=DEFAULT_DURATION_SECONDS,  metavar="SECONDS")
    parser.add_argument("--debug",    action="store_true")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    run(tickers=args.tickers, stream_name=args.stream, interval=args.interval, duration=args.duration)