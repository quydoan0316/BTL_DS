#!/usr/bin/env python3
"""
Preprocess Consumer - Consumer 1 in the pipeline.

Reads raw data from Kafka, performs preprocessing:
- JSON parsing and validation
- Timestamp normalization
- Coordinate conversion (OSGB36 → WGS84)
- Region assignment

Publishes enriched data to the next topic.
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config import (
    KAFKA_BOOTSTRAP,
    TOPIC_RAW,
    TOPIC_ENRICHED,
    GROUP_PREPROCESS,
)
from utils.coordinate_utils import (
    osgb36_to_wgs84,
    determine_region_from_coords,
    safe_float,
    safe_int,
)

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("preprocess-consumer")


class PreprocessConsumer:
    """
    Consumer 1: Data preprocessing and enrichment.
    
    Responsibilities:
    - Read from water-quality-raw
    - Parse and validate JSON
    - Normalize timestamps
    - Convert coordinates
    - Assign regions
    - Publish to water-quality-enriched
    """
    
    def __init__(
        self,
        input_topic: str = TOPIC_RAW,
        output_topic: str = TOPIC_ENRICHED,
        group_id: str = GROUP_PREPROCESS,
        bootstrap_servers: str = KAFKA_BOOTSTRAP,
    ):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        
        self.consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None
        self.message_count = 0
        
        logger.info(
            "PreprocessConsumer: input=%s, output=%s, group=%s",
            input_topic, output_topic, group_id
        )
    
    def connect(self) -> None:
        """Establish Kafka connections."""
        # Consumer
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            max_poll_interval_ms=300000,
            session_timeout_ms=10000,
        )
        
        # Producer for output
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
        )
        
        logger.info("Kafka connections established")
    
    def disconnect(self) -> None:
        """Close Kafka connections."""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        logger.info("Kafka connections closed. Total processed: %d", self.message_count)
    
    def _parse_timestamp(self, raw: Dict[str, Any]) -> Optional[datetime]:
        """
        Parse and normalize timestamp from raw data.
        
        Tries multiple field names and formats.
        """
        ts_raw = (
            raw.get("sample.sampleDateTime") or
            raw.get("sampleDateTime") or
            raw.get("sample_time")
        )
        
        if not ts_raw:
            return None
        
        # Try ISO format first
        try:
            return datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
        
        # Try common format
        try:
            return datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            pass
        
        # Try date only
        try:
            return datetime.strptime(ts_raw, "%Y-%m-%d")
        except (ValueError, TypeError):
            pass
        
        logger.warning("Failed to parse timestamp: %s", ts_raw)
        return None
    
    def _enrich_message(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform raw message into enriched format.
        
        Args:
            raw: Raw message from producer
            
        Returns:
            Enriched message dictionary
        """
        # Extract fields with fallbacks
        easting = raw.get("sample.samplingPoint.easting") or raw.get("easting")
        northing = raw.get("sample.samplingPoint.northing") or raw.get("northing")
        
        # Convert coordinates
        lat, lon = None, None
        if easting and northing:
            lat, lon = osgb36_to_wgs84(easting, northing)
        
        # Get sampling point
        sampling_point = (
            raw.get("sample.samplingPoint.label") or
            raw.get("sample.samplingPoint.notation") or
            raw.get("sampling_point")
        )
        
        # Determine region
        region = sampling_point
        if not region and easting and northing:
            region = determine_region_from_coords(easting, northing)
        
        # Parse timestamp
        sample_time = self._parse_timestamp(raw)
        
        # Parse value
        value = safe_float(raw.get("result"))
        
        # Build enriched message
        enriched = {
            "id": raw.get("@id") or raw.get("id"),
            "sampling_point": sampling_point,
            "sample_time": sample_time.isoformat() if sample_time else None,
            "determinand": raw.get("determinand.label") or raw.get("determinand"),
            "value": value,
            "unit": raw.get("determinand.unit.label") or raw.get("unit"),
            "easting": safe_int(easting),
            "northing": safe_int(northing),
            "latitude": lat,
            "longitude": lon,
            "region": region,
            "producer_region": raw.get("_producer_region"),
            "raw_data": raw,
        }
        
        return enriched
    
    def _publish(self, enriched: Dict[str, Any]) -> bool:
        """Publish enriched message to output topic."""
        if not self.producer:
            return False
        
        try:
            key = enriched.get("sampling_point") or enriched.get("id")
            self.producer.send(
                self.output_topic,
                key=str(key) if key else None,
                value=enriched,
            )
            return True
        except KafkaError as e:
            logger.error("Failed to publish: %s", e)
            return False
    
    def process_message(self, msg) -> None:
        """Process a single Kafka message."""
        try:
            raw = msg.value
            enriched = self._enrich_message(raw)
            
            if self._publish(enriched):
                self.message_count += 1
                
                if self.message_count % 100 == 0:
                    logger.info(
                        "Processed %d messages | Last: %s",
                        self.message_count,
                        enriched.get("sampling_point", "N/A")
                    )
        except Exception as e:
            logger.exception("Error processing message: %s", e)
    
    def run(self) -> None:
        """Main consumer loop."""
        logger.info("=" * 60)
        logger.info("PREPROCESS CONSUMER STARTING")
        logger.info("  Input:  %s", self.input_topic)
        logger.info("  Output: %s", self.output_topic)
        logger.info("  Group:  %s", self.group_id)
        logger.info("=" * 60)
        
        try:
            self.connect()
            
            for msg in self.consumer:
                self.process_message(msg)
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.exception("Consumer error: %s", e)
        finally:
            self.disconnect()


def main():
    """Main entry point."""
    consumer = PreprocessConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
