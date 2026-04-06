"""
stock_producer.py
=================
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
  python producer.py                         # uses defaults (5 tickers, 10 s interval)
  python producer.py --tickers AAPL MSFT GOOGL --interval 5 --duration 300
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

import boto3
import yfinance as yf
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

load_dotenv()  # loads producer/.env if present

DEFAULT_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
DEFAULT_INTERVAL_SECONDS = 10
DEFAULT_DURATION_SECONDS = None  # None = run forever

AWS_REGION        = os.getenv("AWS_REGION", "us-east-1")
STREAM_NAME       = os.getenv("KINESIS_STREAM_NAME", "stock-market-kinesis-dev-kinesis-stream")
MAX_BATCH_SIZE    = 500          # Kinesis PutRecords max = 500 records per call
MAX_BATCH_BYTES   = 5 * 1024 * 1024  # 5 MB per PutRecords call
MAX_RECORD_BYTES  = 1 * 1024 * 1024  # 1 MB per single record


# ─────────────────────────────────────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("stock-producer")


# ─────────────────────────────────────────────────────────────────────────────
#  Data models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class StockQuote:
    """A single stock market data point sent to Kinesis."""
    event_id:          str   = field(default_factory=lambda: str(uuid.uuid4()))
    event_time:        str   = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ticker:            str   = ""
    company_name:      str   = ""
    currency:          str   = "USD"
    exchange:          str   = ""

    # Price fields
    current_price:     Optional[float] = None
    open_price:        Optional[float] = None
    previous_close:    Optional[float] = None
    day_high:          Optional[float] = None
    day_low:           Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low:  Optional[float] = None

    # Volume & market cap
    volume:            Optional[int]   = None
    avg_volume:        Optional[int]   = None
    market_cap:        Optional[int]   = None

    # Derived metrics
    price_change:      Optional[float] = None   # vs previous close
    price_change_pct:  Optional[float] = None   # percentage
    day_range_pct:     Optional[float] = None   # (high-low)/open * 100

    # Fundamental snapshot
    pe_ratio:          Optional[float] = None
    dividend_yield:    Optional[float] = None
    beta:              Optional[float] = None
    eps:               Optional[float] = None

    # Producer metadata
    producer_version:  str = "1.0.0"
    data_source:       str = "yfinance"

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


# ─────────────────────────────────────────────────────────────────────────────
#  Stock Fetcher
# ─────────────────────────────────────────────────────────────────────────────

class StockFetcher:
    """Fetches real-time stock quotes via yfinance."""

    def __init__(self, tickers: list[str]) -> None:
        self.tickers = [t.upper() for t in tickers]
        self._ticker_objects: dict[str, yf.Ticker] = {}

    def _get_ticker(self, symbol: str) -> yf.Ticker:
        if symbol not in self._ticker_objects:
            self._ticker_objects[symbol] = yf.Ticker(symbol)
        return self._ticker_objects[symbol]

    def _safe_float(self, value) -> Optional[float]:
        try:
            v = float(value)
            return round(v, 4) if v == v else None  # NaN check
        except (TypeError, ValueError):
            return None

    def _safe_int(self, value) -> Optional[int]:
        try:
            v = int(value)
            return v
        except (TypeError, ValueError):
            return None

    def fetch_quote(self, symbol: str) -> Optional[StockQuote]:
        """Fetch a single ticker and return a StockQuote dataclass."""
        try:
            ticker = self._get_ticker(symbol)
            info   = ticker.fast_info          # lightweight, faster than .info

            current_price  = self._safe_float(getattr(info, "last_price",    None))
            previous_close = self._safe_float(getattr(info, "previous_close", None))
            open_price     = self._safe_float(getattr(info, "open",           None))
            day_high       = self._safe_float(getattr(info, "day_high",       None))
            day_low        = self._safe_float(getattr(info, "day_low",        None))

            # Derived calculations
            price_change     = None
            price_change_pct = None
            day_range_pct    = None

            if current_price is not None and previous_close:
                price_change     = round(current_price - previous_close, 4)
                price_change_pct = round((price_change / previous_close) * 100, 4)

            if day_high and day_low and open_price and open_price != 0:
                day_range_pct = round(((day_high - day_low) / open_price) * 100, 4)

            # Full info for fundamentals (slower, cached by yfinance)
            full_info = {}
            try:
                full_info = ticker.info or {}
            except Exception:
                pass

            quote = StockQuote(
                ticker         = symbol,
                company_name   = full_info.get("longName", ""),
                currency       = getattr(info, "currency", "USD") or "USD",
                exchange       = full_info.get("exchange", ""),
                current_price  = current_price,
                open_price     = open_price,
                previous_close = previous_close,
                day_high       = day_high,
                day_low        = day_low,
                fifty_two_week_high = self._safe_float(getattr(info, "year_high", None)),
                fifty_two_week_low  = self._safe_float(getattr(info, "year_low",  None)),
                volume         = self._safe_int(getattr(info, "three_month_average_volume", None)),
                avg_volume     = self._safe_int(full_info.get("averageVolume")),
                market_cap     = self._safe_int(getattr(info, "market_cap", None)),
                price_change     = price_change,
                price_change_pct = price_change_pct,
                day_range_pct    = day_range_pct,
                pe_ratio       = self._safe_float(full_info.get("trailingPE")),
                dividend_yield = self._safe_float(full_info.get("dividendYield")),
                beta           = self._safe_float(full_info.get("beta")),
                eps            = self._safe_float(full_info.get("trailingEps")),
            )

            log.debug("Fetched %s: $%s (%.2f%%)", symbol, current_price, price_change_pct or 0)
            return quote

        except Exception as exc:
            log.warning("Failed to fetch %s: %s", symbol, exc)
            return None

    def fetch_all(self) -> list[StockQuote]:
        """Fetch all configured tickers, skipping failures."""
        quotes = []
        for ticker in self.tickers:
            quote = self.fetch_quote(ticker)
            if quote:
                quotes.append(quote)
        return quotes


# ─────────────────────────────────────────────────────────────────────────────
#  Kinesis Producer
# ─────────────────────────────────────────────────────────────────────────────

class KinesisProducer:
    """
    Puts records to a Kinesis Data Stream using PutRecords (batch API).

    Key concepts:
    - Partition key: determines which shard a record lands on.
      Using the ticker symbol distributes load across shards and keeps
      per-ticker ordering within a shard.
    - PutRecords: up to 500 records or 5 MB per call.
      Failed records are retried automatically by this producer.
    """

    def __init__(
        self,
        stream_name: str,
        region: str = AWS_REGION,
        max_retries: int = 3,
    ) -> None:
        self.stream_name = stream_name
        self.max_retries = max_retries

        self.client = boto3.client(
            "kinesis",
            region_name=region,
            # Credentials are resolved automatically:
            #   1. Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
            #   2. ~/.aws/credentials
            #   3. IAM role on EC2/ECS/Lambda
        )

        # Runtime statistics
        self.stats = {
            "records_sent":   0,
            "records_failed": 0,
            "batches_sent":   0,
            "api_errors":     0,
        }

    def _build_kinesis_record(self, quote: StockQuote) -> dict:
        data = quote.to_json().encode("utf-8")
        if len(data) > MAX_RECORD_BYTES:
            raise ValueError(f"Record for {quote.ticker} exceeds 1 MB limit")
        return {
            "Data":         data,
            "PartitionKey": quote.ticker,  # ticker → consistent shard routing
        }

    def _chunk_records(self, records: list[dict]) -> list[list[dict]]:
        """Split records into batches respecting Kinesis limits."""
        batches, current_batch, current_bytes = [], [], 0

        for record in records:
            record_bytes = len(record["Data"])
            if (
                len(current_batch) >= MAX_BATCH_SIZE
                or current_bytes + record_bytes > MAX_BATCH_BYTES
            ):
                batches.append(current_batch)
                current_batch, current_bytes = [], 0
            current_batch.append(record)
            current_bytes += record_bytes

        if current_batch:
            batches.append(current_batch)

        return batches

    def put_records(self, quotes: list[StockQuote]) -> dict:
        """
        Send a list of StockQuote objects to Kinesis using PutRecords.
        Returns a summary dict with success/failure counts.
        """
        if not quotes:
            return {"sent": 0, "failed": 0}

        kinesis_records = []
        for quote in quotes:
            try:
                kinesis_records.append(self._build_kinesis_record(quote))
            except ValueError as e:
                log.error(e)

        batches  = self._chunk_records(kinesis_records)
        total_ok = 0
        total_fail = 0

        for batch_idx, batch in enumerate(batches):
            ok, fail = self._send_batch_with_retry(batch, batch_idx)
            total_ok   += ok
            total_fail += fail

        self.stats["records_sent"]   += total_ok
        self.stats["records_failed"] += total_fail

        log.info(
            "Batch complete → sent=%d failed=%d  (cumulative: sent=%d failed=%d)",
            total_ok, total_fail,
            self.stats["records_sent"], self.stats["records_failed"],
        )

        return {"sent": total_ok, "failed": total_fail}

    def _send_batch_with_retry(
        self, records: list[dict], batch_idx: int
    ) -> tuple[int, int]:
        """Send one batch, retrying any failed records up to max_retries."""
        remaining    = records
        attempt      = 0
        cumulative_ok = 0

        while remaining and attempt <= self.max_retries:
            if attempt > 0:
                sleep_time = 2 ** attempt  # exponential back-off: 2, 4, 8 s
                log.warning(
                    "Retrying %d failed records (attempt %d/%d, sleeping %ds)",
                    len(remaining), attempt, self.max_retries, sleep_time,
                )
                time.sleep(sleep_time)

            try:
                response = self.client.put_records(
                    StreamName=self.stream_name,
                    Records=remaining,
                )
                self.stats["batches_sent"] += 1

                failed_records = []
                for i, result in enumerate(response["Records"]):
                    if "ErrorCode" in result:
                        log.debug(
                            "Record %d failed: %s – %s",
                            i, result["ErrorCode"], result.get("ErrorMessage"),
                        )
                        failed_records.append(remaining[i])

                cumulative_ok += len(remaining) - len(failed_records)
                remaining = failed_records
                attempt  += 1

                if not failed_records:
                    return cumulative_ok, 0   # all records eventually sent

            except (BotoCoreError, ClientError) as exc:
                self.stats["api_errors"] += 1
                log.error("Kinesis API error (batch %d, attempt %d): %s", batch_idx, attempt, exc)
                attempt += 1

        failed_count = len(remaining)
        return cumulative_ok, failed_count

    def describe_stream(self) -> dict:
        """Returns basic stream info for startup validation."""
        try:
            resp = self.client.describe_stream_summary(StreamName=self.stream_name)
            return resp["StreamDescriptionSummary"]
        except (BotoCoreError, ClientError) as exc:
            log.error("Cannot describe stream '%s': %s", self.stream_name, exc)
            raise


# ─────────────────────────────────────────────────────────────────────────────
#  Main loop
# ─────────────────────────────────────────────────────────────────────────────

def run(
    tickers: list[str],
    stream_name: str,
    interval: int,
    duration: Optional[int],
) -> None:
    """
    Main producer loop.

    1. Validate Kinesis stream is accessible.
    2. Every `interval` seconds, fetch quotes for all tickers.
    3. Send quotes to Kinesis via PutRecords.
    4. Repeat until `duration` seconds have elapsed (or forever if None).
    """

    log.info("━" * 60)
    log.info("  Stock Market Kinesis Producer")
    log.info("  Stream  : %s", stream_name)
    log.info("  Tickers : %s", ", ".join(tickers))
    log.info("  Interval: %d s", interval)
    log.info("  Duration: %s", f"{duration} s" if duration else "∞ (until Ctrl+C)")
    log.info("━" * 60)

    fetcher  = StockFetcher(tickers)
    producer = KinesisProducer(stream_name=stream_name)

    # Validate stream connectivity
    try:
        info = producer.describe_stream()
        log.info(
            "✓ Connected to Kinesis stream '%s'  [status=%s, shards=%d]",
            stream_name,
            info.get("StreamStatus"),
            info.get("OpenShardCount", "?"),
        )
    except Exception:
        log.error("✗ Cannot connect to Kinesis stream. Check credentials and stream name.")
        sys.exit(1)

    start_time = time.time()
    iteration  = 0

    try:
        while True:
            iteration += 1
            cycle_start = time.time()

            log.info("── Iteration %d ──────────────────────────────────────", iteration)

            # Fetch quotes
            quotes = fetcher.fetch_all()

            if quotes:
                # Print a nice table to stdout
                print(f"\n{'TICKER':<8} {'PRICE':>10} {'CHG':>9} {'CHG%':>8} {'VOLUME':>14}  COMPANY")
                print("─" * 75)
                for q in quotes:
                    chg_str = f"{q.price_change:+.2f}" if q.price_change is not None else "   N/A"
                    pct_str = f"{q.price_change_pct:+.2f}%" if q.price_change_pct is not None else "   N/A"
                    vol_str = f"{q.volume:,}" if q.volume else "N/A"
                    price_str = f"${q.current_price:.2f}" if q.current_price else "N/A"
                    print(f"{q.ticker:<8} {price_str:>10} {chg_str:>9} {pct_str:>8} {vol_str:>14}  {q.company_name[:30]}")
                print()

                # Send to Kinesis
                result = producer.put_records(quotes)
                log.info("Sent %d/%d records to Kinesis.", result["sent"], len(quotes))
            else:
                log.warning("No quotes fetched this iteration.")

            # Check duration
            elapsed = time.time() - start_time
            if duration and elapsed >= duration:
                log.info("Duration of %d s reached. Stopping.", duration)
                break

            # Sleep for the remainder of the interval
            cycle_elapsed = time.time() - cycle_start
            sleep_time    = max(0.0, interval - cycle_elapsed)
            if sleep_time > 0:
                log.info("Sleeping %.1f s until next poll…", sleep_time)
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        log.info("Interrupted by user (Ctrl+C).")

    finally:
        log.info("━" * 60)
        log.info("Producer stats:")
        for key, value in producer.stats.items():
            log.info("  %-20s : %s", key, value)
        log.info("━" * 60)


# ─────────────────────────────────────────────────────────────────────────────
#  CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream stock market data from Yahoo Finance to AWS Kinesis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python producer.py
  python producer.py --tickers AAPL MSFT TSLA --interval 5
  python producer.py --tickers SPY QQQ --interval 15 --duration 3600
  python producer.py --stream my-custom-stream --tickers NVDA AMD --interval 2
        """,
    )

    parser.add_argument(
        "--tickers",
        nargs="+",
        default=DEFAULT_TICKERS,
        metavar="SYMBOL",
        help=f"Space-separated list of ticker symbols (default: {' '.join(DEFAULT_TICKERS)})",
    )
    parser.add_argument(
        "--stream",
        default=STREAM_NAME,
        metavar="STREAM_NAME",
        help=f"Kinesis stream name (default: {STREAM_NAME} from env KINESIS_STREAM_NAME)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        metavar="SECONDS",
        help=f"Polling interval in seconds (default: {DEFAULT_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION_SECONDS,
        metavar="SECONDS",
        help="Total run duration in seconds (default: run forever)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    run(
        tickers     = args.tickers,
        stream_name = args.stream,
        interval    = args.interval,
        duration    = args.duration,
    )