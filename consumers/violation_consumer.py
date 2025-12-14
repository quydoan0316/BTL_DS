#!/usr/bin/env python3
"""
Violation Consumer - Consumer 3 in the pipeline.

Reads enriched data from Kafka, performs rule-based violation detection:
- Compare values against UK regulatory limits
- Calculate severity (value / limit)
- Assign violation levels
- Publish violations to output topic
- Write to PostgreSQL

Runs independently of other consumers.
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config import (
    KAFKA_BOOTSTRAP,
    TOPIC_ENRICHED,
    TOPIC_VIOLATION,
    GROUP_VIOLATION,
    UK_LIMITS,
    SEVERITY_WARNING,
    SEVERITY_CRITICAL,
    get_limit,
    get_severity_level,
)
from utils.db_writer import DatabaseWriter

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("violation-consumer")


class ViolationConsumer:
    """
    Consumer 3: Rule-based violation detection.
    
    Responsibilities:
    - Compare values to UK limits
    - Calculate severity
    - Detect violations
    - Publish violation events
    - Write to database
    """
    
    def __init__(
        self,
        input_topic: str = TOPIC_ENRICHED,
        output_topic: str = TOPIC_VIOLATION,
        group_id: str = GROUP_VIOLATION,
        bootstrap_servers: str = KAFKA_BOOTSTRAP,
    ):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        
        self.consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None
        self.db_writer: Optional[DatabaseWriter] = None
        
        self.message_count = 0
        self.violation_count = 0
        self.warning_count = 0
        
        logger.info(
            "ViolationConsumer: input=%s, output=%s",
            input_topic, output_topic
        )
    
    def connect(self) -> None:
        """Establish all connections."""
        # Kafka Consumer
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
        
        # Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
        )
        
        # Database Writer
        self.db_writer = DatabaseWriter(table_name="water_quality")
        self.db_writer.start()
        
        logger.info("All connections established")
    
    def disconnect(self) -> None:
        """Close all connections."""
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        if self.db_writer:
            self.db_writer.stop()
        
        logger.info(
            "Connections closed. Messages: %d, Violations: %d, Warnings: %d",
            self.message_count, self.violation_count, self.warning_count
        )
    
    def _check_violation(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Check if a measurement violates UK limits.
        
        Args:
            data: Enriched message data
            
        Returns:
            Violation details dictionary
        """
        determinand = data.get("determinand")
        value = data.get("value")
        
        # Default response
        result = {
            "is_violation": False,
            "violation_level": "none",
            "limit": None,
            "severity": None,
        }
        
        if determinand is None or value is None:
            return result
        
        # Get UK limit for this determinand
        limit = get_limit(determinand)
        
        if limit is None:
            return result
        
        result["limit"] = limit
        
        # Calculate severity (ratio of value to limit)
        severity = value / limit if limit > 0 else None
        result["severity"] = severity
        
        # Determine violation level
        if severity is not None:
            level = get_severity_level(value, limit)
            result["violation_level"] = level
            result["is_violation"] = (severity >= SEVERITY_CRITICAL)
        
        return result
    
    def _build_violation_message(
        self,
        data: Dict[str, Any],
        violation_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the violation event message."""
        return {
            "id": data.get("id"),
            "sampling_point": data.get("sampling_point"),
            "sample_time": data.get("sample_time"),
            "determinand": data.get("determinand"),
            "value": data.get("value"),
            "unit": data.get("unit"),
            "limit": violation_info.get("limit"),
            "severity": violation_info.get("severity"),
            "is_violation": violation_info.get("is_violation"),
            "violation_level": violation_info.get("violation_level"),
            "region": data.get("region"),
            "producer_region": data.get("producer_region"),
            "latitude": data.get("latitude"),
            "longitude": data.get("longitude"),
        }
    
    def process_message(self, msg) -> None:
        """Process a single message."""
        try:
            data = msg.value
            self.message_count += 1
            
            # Check for violation
            violation_info = self._check_violation(data)
            
            # Track statistics
            level = violation_info.get("violation_level")
            if level == "critical":
                self.violation_count += 1
            elif level == "warning":
                self.warning_count += 1
            
            # Always publish to violation topic (for monitoring)
            # Could filter to only publish actual violations
            if violation_info.get("limit") is not None:
                violation_msg = self._build_violation_message(data, violation_info)
                
                if self.producer:
                    self.producer.send(
                        self.output_topic,
                        key=data.get("sampling_point"),
                        value=violation_msg,
                    )
            
            # Write to database with violation info
            db_record = dict(data)
            db_record["violation"] = violation_info.get("is_violation")
            db_record["violations"] = {
                data.get("determinand"): {
                    "value": data.get("value"),
                    "limit": violation_info.get("limit"),
                    "severity": violation_info.get("severity"),
                    "is_violation": violation_info.get("is_violation"),
                    "level": violation_info.get("violation_level"),
                }
            } if violation_info.get("limit") else None
            
            if self.db_writer:
                self.db_writer.write(db_record)
            
            # Log violations
            if level == "critical":
                logger.warning(
                    "VIOLATION: %s = %.2f (limit: %.2f, severity: %.2f) at %s",
                    data.get("determinand"),
                    data.get("value", 0),
                    violation_info.get("limit", 0),
                    violation_info.get("severity", 0),
                    data.get("sampling_point"),
                )
            
            if self.message_count % 100 == 0:
                logger.info(
                    "Processed: %d | Violations: %d | Warnings: %d",
                    self.message_count, self.violation_count, self.warning_count
                )
                
        except Exception as e:
            logger.exception("Error processing message: %s", e)
    
    def run(self) -> None:
        """Main consumer loop."""
        logger.info("=" * 60)
        logger.info("VIOLATION CONSUMER STARTING")
        logger.info("  Input:  %s", self.input_topic)
        logger.info("  Output: %s", self.output_topic)
        logger.info("  UK Limits configured: %d determinands", len(UK_LIMITS))
        logger.info("=" * 60)
        
        # Log configured limits
        for det, limit in list(UK_LIMITS.items())[:5]:
            logger.info("    %s: %.3f", det, limit)
        if len(UK_LIMITS) > 5:
            logger.info("    ... and %d more", len(UK_LIMITS) - 5)
        
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
    consumer = ViolationConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
