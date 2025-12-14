#!/usr/bin/env python3
"""
Multi-Producer Orchestrator for Water Quality IoT Simulation.

Runs all regional producers in parallel, simulating a distributed
IoT sensor network sending data to Kafka.

Usage:
    python run_all_producers.py
    python run_all_producers.py --regions AN SO SW
    python run_all_producers.py --limit 100
"""

import os
import sys
import argparse
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from region_producer import RegionProducer, REGION_PREFIXES, REGION_DELAYS

# -----------------------------
# Configuration
# -----------------------------
CSV_FILE = os.getenv("CSV_FILE", "../2025-C.csv")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("multi-producer")

# Global stop event for graceful shutdown
stop_event = threading.Event()


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    logger.info("Received shutdown signal, stopping all producers...")
    stop_event.set()


def run_region_producer(
    region: str,
    csv_file: str,
    limit: Optional[int],
    bootstrap_servers: Optional[str]
) -> dict:
    """
    Run a single region producer.
    
    Args:
        region: Region prefix
        csv_file: Path to CSV file
        limit: Message limit per producer
        bootstrap_servers: Kafka bootstrap servers
        
    Returns:
        Result dictionary with region, status, and message count
    """
    result = {
        "region": region,
        "status": "unknown",
        "messages_sent": 0,
        "error": None
    }
    
    try:
        kwargs = {}
        if bootstrap_servers:
            kwargs["bootstrap_servers"] = bootstrap_servers
        
        producer = RegionProducer(
            region_prefix=region,
            csv_file=csv_file,
            **kwargs
        )
        
        producer.connect()
        
        sent_count = 0
        for row in producer.read_csv_filtered():
            if stop_event.is_set():
                logger.info("[%s] Stop signal received", region)
                break
            
            if limit and sent_count >= limit:
                break
            
            if producer.send_message(row):
                sent_count += 1
            
            # Use region-specific delay
            import time
            time.sleep(producer.delay_seconds)
        
        producer.disconnect()
        
        result["status"] = "completed" if not stop_event.is_set() else "stopped"
        result["messages_sent"] = sent_count
        
    except Exception as e:
        logger.exception("[%s] Producer failed: %s", region, e)
        result["status"] = "failed"
        result["error"] = str(e)
    
    return result


def run_all_producers(
    regions: List[str],
    csv_file: str = CSV_FILE,
    limit: Optional[int] = None,
    bootstrap_servers: Optional[str] = None,
    max_workers: int = MAX_WORKERS
) -> List[dict]:
    """
    Run multiple region producers in parallel.
    
    Args:
        regions: List of region prefixes to run
        csv_file: Path to CSV file
        limit: Message limit per producer
        bootstrap_servers: Kafka bootstrap servers
        max_workers: Maximum concurrent producers
        
    Returns:
        List of result dictionaries
    """
    logger.info("=" * 60)
    logger.info("MULTI-PRODUCER ORCHESTRATOR")
    logger.info("=" * 60)
    logger.info("Regions: %s", regions)
    logger.info("CSV File: %s", csv_file)
    logger.info("Limit per region: %s", limit or "Unlimited")
    logger.info("Max workers: %d", max_workers)
    logger.info("=" * 60)
    
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all producers
        futures = {
            executor.submit(
                run_region_producer,
                region,
                csv_file,
                limit,
                bootstrap_servers
            ): region
            for region in regions
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            region = futures[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(
                    "[%s] Finished: status=%s, messages=%d",
                    region, result["status"], result["messages_sent"]
                )
            except Exception as e:
                logger.exception("[%s] Future failed: %s", region, e)
                results.append({
                    "region": region,
                    "status": "failed",
                    "messages_sent": 0,
                    "error": str(e)
                })
    
    return results


def print_summary(results: List[dict]) -> None:
    """Print summary of all producer runs."""
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    total_messages = 0
    for r in results:
        status_icon = "✓" if r["status"] == "completed" else "✗"
        print(f"  {status_icon} {r['region']}: {r['messages_sent']} messages ({r['status']})")
        total_messages += r["messages_sent"]
        if r.get("error"):
            print(f"      Error: {r['error']}")
    
    print("-" * 60)
    print(f"  TOTAL: {total_messages} messages")
    print("=" * 60)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run multiple regional IoT producers in parallel"
    )
    parser.add_argument(
        "--regions", "-r",
        nargs="+",
        default=list(REGION_PREFIXES.keys()),
        choices=list(REGION_PREFIXES.keys()),
        help="Regions to run (default: all)"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=None,
        help="Maximum messages per producer (default: unlimited)"
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
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--workers", "-w",
        type=int,
        default=MAX_WORKERS,
        help=f"Max concurrent producers (default: {MAX_WORKERS})"
    )
    return parser.parse_args()


def main():
    """Main entry point."""
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    args = parse_args()
    
    print("\n" + "=" * 60)
    print("WATER QUALITY IoT MULTI-PRODUCER SIMULATION")
    print("=" * 60)
    print(f"  Regions: {', '.join(args.regions)}")
    print(f"  CSV: {args.csv}")
    print(f"  Limit: {args.limit or 'Unlimited'} per region")
    print(f"  Workers: {args.workers}")
    print("=" * 60)
    print("\nStarting producers... (Press Ctrl+C to stop)\n")
    
    try:
        results = run_all_producers(
            regions=args.regions,
            csv_file=args.csv,
            limit=args.limit,
            bootstrap_servers=args.kafka,
            max_workers=args.workers
        )
        print_summary(results)
        
    except KeyboardInterrupt:
        print("\n[!] Interrupted by user")
        stop_event.set()
    except Exception as e:
        logger.exception("Orchestrator failed: %s", e)
        raise


if __name__ == "__main__":
    main()
