#!/usr/bin/env python3
"""
Region-specific Producer for Water Quality IoT Simulation.

This producer simulates IoT sensors from a specific geographic region,
reading data from CSV and streaming to Kafka with configurable delays.

Usage:
    python region_producer.py --region AN --delay 1.0
    python region_producer.py --region SO --delay 0.8
"""

import os
import csv
import time
import argparse
import logging
from typing import Generator, Dict, Any, Optional

from base_producer import BaseProducer

# -----------------------------
# Configuration
# -----------------------------
CSV_FILE = os.getenv("CSV_FILE", "../2025-C.csv")
DEFAULT_DELAY = float(os.getenv("SLEEP_SECONDS", "1.0"))

# Region prefix mapping
REGION_PREFIXES = {
    "AN": "Anglian",
    "SO": "Southern", 
    "SW": "South West",
    "NW": "North West",
    "MD": "Midlands/Other"
}

# Different delay profiles per region (simulating real-world IoT variation)
REGION_DELAYS = {
    "AN": 1.0,   # Anglian - moderate
    "SO": 0.8,   # Southern - faster
    "SW": 1.2,   # South West - slower (rural)
    "NW": 0.9,   # North West - moderate-fast
    "MD": 1.5,   # Midlands - slowest (mixed)
}

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("region-producer")


class RegionProducer(BaseProducer):
    """
    Producer for a specific geographic region.
    
    Filters CSV data by sampling point prefix and streams to Kafka
    with region-specific delays to simulate real IoT conditions.
    """
    
    def __init__(
        self,
        region_prefix: str,
        csv_file: str = CSV_FILE,
        delay_seconds: Optional[float] = None,
        **kwargs
    ):
        """
        Initialize region producer.
        
        Args:
            region_prefix: Region identifier (e.g., "AN", "SO")
            csv_file: Path to the CSV data file
            delay_seconds: Delay between messages (uses region default if None)
            **kwargs: Additional arguments for BaseProducer
        """
        super().__init__(region_prefix, **kwargs)
        
        self.csv_file = csv_file
        self.delay_seconds = delay_seconds or REGION_DELAYS.get(region_prefix, DEFAULT_DELAY)
        self.region_name = REGION_PREFIXES.get(region_prefix, "Unknown")
        
        logger.info(
            "RegionProducer initialized: region=%s (%s), delay=%.2fs, csv=%s",
            region_prefix, self.region_name, self.delay_seconds, csv_file
        )
    
    def _matches_region(self, row: Dict[str, Any]) -> bool:
        """
        Check if a CSV row belongs to this region.
        
        Args:
            row: CSV row as dictionary
            
        Returns:
            True if the sampling point matches this region's prefix
        """
        sampling_point = row.get("sample.samplingPoint.notation", "")
        
        if self.region_prefix == "MD":
            # Midlands catches all other prefixes
            known_prefixes = ["AN-", "SO-", "SW-", "NW-"]
            return not any(sampling_point.startswith(p) for p in known_prefixes)
        
        return sampling_point.startswith(f"{self.region_prefix}-")
    
    def read_csv_filtered(self) -> Generator[Dict[str, Any], None, None]:
        """
        Read CSV file, filter by region, sort by timestamp, and yield rows.
        
        Data is sorted by sample.sampleDateTime to ensure chronological order
        for proper time series processing.
        
        Yields:
            Dictionary for each matching row, sorted by timestamp
        """
        try:
            logger.info("Loading CSV file: %s", self.csv_file)
            
            with open(self.csv_file, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                all_rows = list(reader)
            
            logger.info("Loaded %d total rows from CSV", len(all_rows))
            
            # Filter by region
            matched_rows = [row for row in all_rows if self._matches_region(row)]
            logger.info("Filtered to %d rows for region %s", len(matched_rows), self.region_prefix)
            
            # Sort by sample.sampleDateTime for proper time series order
            # Timestamp format: 2025-01-13T12:04:00
            from datetime import datetime
            
            def parse_timestamp(row):
                ts_str = row.get("sample.sampleDateTime") or row.get("sampleDateTime") or ""
                try:
                    # Parse ISO format: 2025-01-13T12:04:00
                    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    # Return min datetime if parsing fails
                    return datetime.min
            
            matched_rows.sort(key=parse_timestamp)
            logger.info("Sorted %d rows by sample.sampleDateTime (chronological order)", len(matched_rows))
            
            # Yield sorted rows
            for row in matched_rows:
                yield row
                
        except FileNotFoundError:
            logger.error("CSV file not found: %s", self.csv_file)
            raise
        except Exception as e:
            logger.exception("Error reading CSV: %s", e)
            raise
    
    def stream_data(self, limit: Optional[int] = None) -> None:
        """
        Stream data from CSV to Kafka with delays.
        
        Args:
            limit: Maximum number of messages to send (None for all)
        """
        logger.info(
            "Starting data stream for region %s (%s)",
            self.region_prefix, self.region_name
        )
        
        sent_count = 0
        
        for row in self.read_csv_filtered():
            if limit and sent_count >= limit:
                logger.info("Reached limit of %d messages", limit)
                break
            
            success = self.send_message(row)
            
            if success:
                sent_count += 1
                
                # Log every 50 messages
                if sent_count % 50 == 0:
                    sampling_point = row.get("sample.samplingPoint.notation", "N/A")
                    determinand = row.get("determinand.label", "N/A")
                    logger.info(
                        "[%s] Sent %d | Last: %s / %s",
                        self.region_prefix, sent_count, sampling_point, determinand
                    )
            
            # Simulate IoT transmission delay
            time.sleep(self.delay_seconds)
        
        logger.info(
            "Stream complete for region %s: %d messages sent",
            self.region_prefix, sent_count
        )
    
    def run(self, limit: Optional[int] = None) -> None:
        """
        Main entry point - connect, stream, disconnect.
        
        Args:
            limit: Maximum number of messages to send
        """
        try:
            self.connect()
            self.stream_data(limit=limit)
        finally:
            self.disconnect()


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Region-specific Water Quality IoT Producer"
    )
    parser.add_argument(
        "--region", "-r",
        type=str,
        required=True,
        choices=list(REGION_PREFIXES.keys()),
        help="Region prefix (AN, SO, SW, NW, MD)"
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        default=None,
        help="Delay between messages in seconds (default: region-specific)"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Maximum number of messages to send (default: unlimited)"
    )
    parser.add_argument(
        "--csv", "-c",
        type=str,
        default=CSV_FILE,
        help=f"Path to CSV file (default: {CSV_FILE})"
    )
    parser.add_argument(
        "--kafka", "-k",
        type=str,
        default=None,
        help="Kafka bootstrap servers (default: from env or localhost:9092)"
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()
    
    print("=" * 60)
    print(f"WATER QUALITY IoT PRODUCER - Region: {args.region}")
    print(f"  Region Name: {REGION_PREFIXES.get(args.region, 'Unknown')}")
    print(f"  CSV File: {args.csv}")
    print(f"  Delay: {args.delay or REGION_DELAYS.get(args.region, DEFAULT_DELAY)}s")
    print(f"  Limit: {args.limit or 'Unlimited'}")
    print("=" * 60)
    
    kwargs = {}
    if args.kafka:
        kwargs["bootstrap_servers"] = args.kafka
    
    producer = RegionProducer(
        region_prefix=args.region,
        csv_file=args.csv,
        delay_seconds=args.delay,
        **kwargs
    )
    
    try:
        producer.run(limit=args.limit)
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
    except Exception as e:
        logger.exception("Producer failed: %s", e)
        raise


if __name__ == "__main__":
    main()
