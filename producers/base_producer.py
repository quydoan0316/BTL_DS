#!/usr/bin/env python3
"""
Base Producer for Water Quality IoT Simulation.

This module provides the base class for all region-specific producers.
It handles Kafka connection, message serialization, and error handling.
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

# -----------------------------
# Configuration
# -----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "water-quality-raw")

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("base-producer")


class BaseProducer:
    """
    Base class for IoT sensor producers.
    
    Responsibilities:
    - Kafka connection management
    - Message serialization (JSON)
    - Partition key handling (sampling_point)
    - Error handling and retries
    
    NOTE: Producer does NOT perform any data processing.
    All processing is done by downstream consumers.
    """
    
    def __init__(
        self,
        region_prefix: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP,
        topic: str = KAFKA_TOPIC_RAW
    ):
        """
        Initialize the producer.
        
        Args:
            region_prefix: Region identifier (e.g., "AN", "SO", "SW", "NW", "MD")
            bootstrap_servers: Kafka bootstrap servers
            topic: Target Kafka topic
        """
        self.region_prefix = region_prefix
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer: Optional[KafkaProducer] = None
        self.message_count = 0
        
        logger.info(
            "Initializing producer for region=%s, topic=%s, servers=%s",
            region_prefix, topic, bootstrap_servers
        )
    
    def connect(self) -> None:
        """Establish connection to Kafka broker."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",  # Wait for all replicas
                retries=3,
                retry_backoff_ms=500,
                max_in_flight_requests_per_connection=1,  # Ensure ordering
            )
            logger.info("Connected to Kafka successfully")
        except KafkaError as e:
            logger.error("Failed to connect to Kafka: %s", e)
            raise
    
    def disconnect(self) -> None:
        """Close Kafka connection gracefully."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info(
                "Producer disconnected. Total messages sent: %d",
                self.message_count
            )
    
    def _enrich_message(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add producer metadata to the raw message.
        
        Args:
            raw_data: Original data from CSV
            
        Returns:
            Enriched message with producer metadata
        """
        enriched = dict(raw_data)
        enriched["_producer_region"] = self.region_prefix
        enriched["_producer_timestamp"] = datetime.now(timezone.utc).isoformat()
        return enriched
    
    def _get_partition_key(self, data: Dict[str, Any]) -> Optional[str]:
        """
        Extract partition key from message data.
        
        Uses sampling_point as key to ensure all messages from the same
        sensor go to the same partition, maintaining ordering.
        
        Args:
            data: Message data
            
        Returns:
            Partition key string or None
        """
        # Try different field names for sampling point
        key = (
            data.get("sample.samplingPoint.notation") or
            data.get("sample.samplingPoint") or
            data.get("sampling_point") or
            data.get("@id")
        )
        return str(key) if key else None
    
    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Send a single message to Kafka.
        
        Args:
            data: Message data to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.producer:
            logger.error("Producer not connected. Call connect() first.")
            return False
        
        try:
            enriched_data = self._enrich_message(data)
            partition_key = self._get_partition_key(data)
            
            future = self.producer.send(
                self.topic,
                key=partition_key,
                value=enriched_data
            )
            
            # Wait for send to complete (sync mode for reliability)
            future.get(timeout=10)
            
            self.message_count += 1
            
            if self.message_count % 100 == 0:
                logger.info(
                    "[%s] Sent %d messages",
                    self.region_prefix, self.message_count
                )
            
            return True
            
        except KafkaError as e:
            logger.error("Failed to send message: %s", e)
            return False
        except Exception as e:
            logger.exception("Unexpected error sending message: %s", e)
            return False
    
    def send_batch(self, messages: list) -> int:
        """
        Send multiple messages in batch.
        
        Args:
            messages: List of message data dictionaries
            
        Returns:
            Number of successfully sent messages
        """
        success_count = 0
        for msg in messages:
            if self.send_message(msg):
                success_count += 1
        
        # Ensure all messages are flushed
        if self.producer:
            self.producer.flush()
        
        return success_count
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False
