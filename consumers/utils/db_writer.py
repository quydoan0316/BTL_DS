"""
Thread-safe database writer with batch insert and reconnection logic.

Designed for high-throughput Kafka consumer use cases.
"""

import queue
import threading
import logging
import time
from typing import Dict, Any, List, Optional, Callable

import psycopg2
from psycopg2.extras import Json, execute_values
from psycopg2 import OperationalError, InterfaceError, DatabaseError

from .config import CONN_STR, DB_BATCH_SIZE, DB_QUEUE_SIZE

# -----------------------------
# Logging
# -----------------------------
logger = logging.getLogger("db-writer")


class DatabaseWriter:
    """
    Thread-safe database writer with batch inserts.

    Features:
    - Background thread for async writes
    - Batch inserts for efficiency
    - Automatic reconnection on failure
    - Queue-based buffering
    """

    def __init__(
        self,
        conn_str: str = CONN_STR,
        batch_size: int = DB_BATCH_SIZE,
        queue_size: int = DB_QUEUE_SIZE,
        table_name: str = "water_quality"
    ):
        """
        Initialize the database writer.
        
        Args:
            conn_str: PostgreSQL connection string
            batch_size: Number of records to batch before insert
            queue_size: Maximum queue size before dropping messages
            table_name: Target table name
        """
        self.conn_str = conn_str
        self.batch_size = batch_size
        self.table_name = table_name
        
        self._queue: queue.Queue = queue.Queue(maxsize=queue_size)
        self._stop_event = threading.Event()
        self._conn = None  # psycopg2 connection object
        self._worker_thread: Optional[threading.Thread] = None
        
        logger.info(
            "DatabaseWriter initialized: table=%s, batch_size=%d",
            table_name, batch_size
        )

    def _connect(self):
        """Establish database connection with retries."""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            if self._stop_event.is_set():
                raise RuntimeError("Stop event set during connection")
            
            try:
                conn = psycopg2.connect(self.conn_str)
                conn.autocommit = False
                logger.info("Database connection established")
                return conn
            except Exception as e:
                logger.warning(
                    "Connection attempt %d/%d failed: %s",
                    attempt + 1, max_retries, e
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        
        raise RuntimeError("Failed to connect to database after retries")

    def _ensure_connection(self) -> None:
        """Ensure database connection is active."""
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
            self._ensure_table()
    def _ensure_table(self) -> None:
        """Create table if not exists."""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id TEXT PRIMARY KEY,
            sampling_point TEXT,
            sample_time TIMESTAMP,
            determinand TEXT,
            value DOUBLE PRECISION,
            unit TEXT,
            violation BOOLEAN,
            violations JSONB,
            easting INTEGER,
            northing INTEGER,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            region TEXT,
            raw_data JSONB,
            predicted_value DOUBLE PRECISION,
            predicted_model TEXT,
            producer_region TEXT,
            inserted_at TIMESTAMP DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_time ON {self.table_name}(sample_time);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_region ON {self.table_name}(region);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_det ON {self.table_name}(determinand);
        CREATE INDEX IF NOT EXISTS idx_{self.table_name}_violation ON {self.table_name}(violation);
        """
        
        try:
            cur = self._conn.cursor()
            cur.execute(create_sql)
            self._conn.commit()
            cur.close()
            logger.info("Table %s ensured", self.table_name)
        except Exception as e:
            logger.error("Failed to create table: %s", e)
            raise

    def _build_insert_params(self, records: List[Dict[str, Any]]) -> List[tuple]:
        """Convert record dictionaries to tuple format for insert."""
        params = []
        for r in records:
            params.append((
                r.get("id"),
                r.get("sampling_point"),
                r.get("sample_time"),
                r.get("determinand"),
                r.get("value"),
                r.get("unit"),
                r.get("violation"),
                Json(r.get("violations")) if r.get("violations") else None,
                r.get("easting"),
                r.get("northing"),
                r.get("latitude"),
                r.get("longitude"),
                r.get("region"),
                Json(r.get("raw_data")) if r.get("raw_data") else None,
                r.get("predicted_value"),
                r.get("predicted_model"),
                r.get("producer_region"),
            ))
        return params

    def _write_batch(self, records: List[Dict[str, Any]]) -> int:
        """
        Write a batch of records to database.
        
        Returns:
            Number of records written
        """
        if not records:
            return 0
        
        self._ensure_connection()
        
        sql = f"""
        INSERT INTO {self.table_name}
        (id, sampling_point, sample_time, determinand, value, unit,
            violation, violations, easting, northing, latitude, longitude,
    region, raw_data, predicted_value, predicted_model, producer_region)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            sampling_point = COALESCE(EXCLUDED.sampling_point, {self.table_name}.sampling_point),
            sample_time = COALESCE(EXCLUDED.sample_time, {self.table_name}.sample_time),
            determinand = COALESCE(EXCLUDED.determinand, {self.table_name}.determinand),
            value = COALESCE(EXCLUDED.value, {self.table_name}.value),
            unit = COALESCE(EXCLUDED.unit, {self.table_name}.unit),
            violation = COALESCE(EXCLUDED.violation, {self.table_name}.violation),
            violations = COALESCE(EXCLUDED.violations, {self.table_name}.violations),
            easting = COALESCE(EXCLUDED.easting, {self.table_name}.easting),
            northing = COALESCE(EXCLUDED.northing, {self.table_name}.northing),
            latitude = COALESCE(EXCLUDED.latitude, {self.table_name}.latitude),
            longitude = COALESCE(EXCLUDED.longitude, {self.table_name}.longitude),
            region = COALESCE(EXCLUDED.region, {self.table_name}.region),
            raw_data = COALESCE(EXCLUDED.raw_data, {self.table_name}.raw_data),
            predicted_value = COALESCE(EXCLUDED.predicted_value, {self.table_name}.predicted_value),
            predicted_model = COALESCE(EXCLUDED.predicted_model, {self.table_name}.predicted_model),
            producer_region = COALESCE(EXCLUDED.producer_region, {self.table_name}.producer_region);
        """
        
        try:
            params = self._build_insert_params(records)
            cur = self._conn.cursor()
            execute_values(cur, sql, params, page_size=100)
            self._conn.commit()
            cur.close()
            return len(records)
        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.error("Database error during batch write: %s", e)
            self._conn = None  # Force reconnection
            raise

    def _worker_loop(self) -> None:
        """Background worker loop for processing queue."""
        logger.info("Database writer worker started")
        
        while not self._stop_event.is_set():
            batch = []
            
            try:
                # Get first item with timeout
                item = self._queue.get(timeout=1)
                batch.append(item)
                
                # Collect more items up to batch size
                while len(batch) < self.batch_size:
                    try:
                        batch.append(self._queue.get_nowait())
                    except queue.Empty:
                        break
                
                # Write batch
                count = self._write_batch(batch)
                logger.debug("Wrote batch of %d records", count)
                
            except queue.Empty:
                continue
            except Exception as e:
                logger.exception("Worker error: %s", e)
                time.sleep(1)
    # Flush remaining items on shutdown
        self._flush_remaining()
        logger.info("Database writer worker stopped")

    def _flush_remaining(self) -> None:
        """Flush remaining items in queue on shutdown."""
        remaining = []
        while not self._queue.empty():
            try:
                remaining.append(self._queue.get_nowait())
            except queue.Empty:
                break
        
        if remaining:
            try:
                count = self._write_batch(remaining)
                logger.info("Flushed %d remaining records", count)
            except Exception as e:
                logger.error("Failed to flush remaining records: %s", e)

    def start(self) -> None:
        """Start the background writer thread."""
        if self._worker_thread and self._worker_thread.is_alive():
            logger.warning("Writer already running")
            return
        
        self._stop_event.clear()
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            daemon=True,
            name="db-writer"
        )
        self._worker_thread.start()
        logger.info("Database writer started")

    def stop(self) -> None:
        """Stop the background writer thread."""
        self._stop_event.set()
        if self._worker_thread:
            self._worker_thread.join(timeout=10)
        if self._conn:
            self._conn.close()
        logger.info("Database writer stopped")

    def write(self, record: Dict[str, Any]) -> bool:
        """
        Queue a record for writing.
        
        Args:
            record: Record dictionary to write
            
        Returns:
            True if queued successfully, False if queue is full
        """
        try:
            self._queue.put_nowait(record)
            return True
        except queue.Full:
            logger.warning("Queue full, dropping record: %s", record.get("id"))
            return False

    def write_many(self, records: List[Dict[str, Any]]) -> int:
        """
        Queue multiple records for writing.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Number of records successfully queued
        """
        count = 0
        for record in records:
            if self.write(record):
                count += 1
        return count

    @property
    def queue_size(self) -> int:
        """Current number of items in queue."""
        return self._queue.qsize()

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop()
        return False