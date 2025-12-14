#!/usr/bin/env python3
"""
Online ML Consumer - Consumer 2 in the pipeline.

Reads enriched data from Kafka, performs ML prediction:
- Filters for BOD ATU determinand only
- Online learning with SGDRegressor
- Background retraining with RandomForest
- Publishes predictions to output topic
- Writes to PostgreSQL

Runs independently of other consumers.
"""

import os
import sys
import json
import math
import time
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

import numpy as np
from sklearn.linear_model import SGDRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import joblib

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config import (
    KAFKA_BOOTSTRAP,
    TOPIC_ENRICHED,
    TOPIC_PREDICTION,
    GROUP_ML,
    ML_TARGET_DETERMINAND,
    MODEL_DIR,
    RF_MODEL_PATH,
    SCALER_PATH,
    TRAIN_CHECK_INTERVAL,
    TRAIN_THRESHOLD,
    TRAIN_FETCH_LIMIT,
    CONN_STR,
)
from utils.db_writer import DatabaseWriter

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("ml-consumer")

# Ensure model directory exists
os.makedirs(MODEL_DIR, exist_ok=True)


class OnlineMLConsumer:
    """
    Consumer 2: Online ML prediction for BOD ATU.
    
    Responsibilities:
    - Filter for BOD ATU only
    - Online learning (SGDRegressor)
    - Background retraining (RandomForest)
    - Publish predictions
    - Write to database
    """
    
    def __init__(
        self,
        input_topic: str = TOPIC_ENRICHED,
        output_topic: str = TOPIC_PREDICTION,
        group_id: str = GROUP_ML,
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
        self.prediction_count = 0
        
        # Online model state
        self._online_lock = threading.Lock()
        self._online_model = SGDRegressor(max_iter=1000, tol=1e-3)
        self._online_initialized = False
        
        # Welford's algorithm for online normalization
        self._welford_count = 0
        self._welford_mean = 0.0
        self._welford_M2 = 0.0
        
        # Offline model (RandomForest)
        self._model_lock = threading.Lock()
        self._offline_model: Optional[RandomForestRegressor] = None
        self._offline_scaler: Optional[StandardScaler] = None
        
        # Background trainer
        self._stop_event = threading.Event()
        self._trainer_thread: Optional[threading.Thread] = None
        
        logger.info(
            "OnlineMLConsumer: input=%s, output=%s, target=%s",
            input_topic, output_topic, ML_TARGET_DETERMINAND
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
        
        # Start background trainer
        self._trainer_thread = threading.Thread(
            target=self._background_trainer,
            daemon=True,
            name="ml-trainer"
        )
        self._trainer_thread.start()
        
        logger.info("All connections established")
    
    def disconnect(self) -> None:
        """Close all connections."""
        self._stop_event.set()
        
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()
            self.producer.close()
        if self.db_writer:
            self.db_writer.stop()
        
        logger.info(
            "Connections closed. Messages: %d, Predictions: %d",
            self.message_count, self.prediction_count
        )
    
    # -------------------------
    # Online Learning
    # -------------------------
    def _welford_update(self, x: float) -> None:
        """Update running statistics using Welford's algorithm."""
        self._welford_count += 1
        if self._welford_count == 1:
            self._welford_mean = x
            self._welford_M2 = 0.0
        else:
            delta = x - self._welford_mean
            self._welford_mean += delta / self._welford_count
            delta2 = x - self._welford_mean
            self._welford_M2 += delta * delta2
    
    def _welford_std(self) -> float:
        """Get running standard deviation."""
        if self._welford_count < 2:
            return 1.0
        return math.sqrt(self._welford_M2 / (self._welford_count - 1))
    
    def _online_update_and_predict(
        self,
        sample_time: datetime,
        value: float
    ) -> Optional[float]:
        """
        Update online SGD model and return prediction.
        
        Uses normalized unix timestamp as feature.
        """
        ts = sample_time.timestamp()
        
        with self._online_lock:
            self._welford_update(ts)
            std = max(self._welford_std(), 1e-6)
            
            X = np.array([[(ts - self._welford_mean) / std]])
            y = np.array([value])
            
            try:
                if not self._online_initialized:
                    self._online_model.partial_fit(X, y)
                    self._online_initialized = True
                else:
                    self._online_model.partial_fit(X, y)
                
                return float(self._online_model.predict(X)[0])
            except Exception as e:
                logger.exception("Online model error: %s", e)
                return None
    
    # -------------------------
    # Offline Model (RandomForest)
    # -------------------------
    def _predict_with_best_model(
        self,
        sample_time: datetime,
        prev_value: Optional[float]
    ) -> Tuple[Optional[float], str]:
        """
        Make prediction using best available model.
        
        Priority: offline_model > online_model
        """
        if sample_time is None:
            return None, "none"
        
        ts = sample_time.timestamp()
        
        # Try offline model first
        with self._model_lock:
            if self._offline_model and self._offline_scaler:
                try:
                    hour = sample_time.hour
                    dow = sample_time.weekday()
                    month = sample_time.month
                    prev = prev_value if prev_value is not None else 0.0
                    
                    X_raw = np.array([[ts, prev, hour, dow, month]])
                    X_scaled = self._offline_scaler.transform(X_raw)
                    pred = float(self._offline_model.predict(X_scaled)[0])
                    
                    return pred, "random_forest"
                except Exception as e:
                    logger.exception("Offline model error: %s", e)
        
        # Fallback to online model
        try:
            prev = prev_value if prev_value is not None else 0.0
            pred = self._online_update_and_predict(sample_time, prev)
            return pred, "online_sgd"
        except Exception as e:
            logger.exception("Online prediction fallback failed: %s", e)
            return None, "none"
    
    def _background_trainer(self) -> None:
        """Background thread for periodic model retraining."""
        logger.info("Background trainer started")
        
        import psycopg2
        
        while not self._stop_event.is_set():
            try:
                # Check data count
                conn = psycopg2.connect(CONN_STR)
                cur = conn.cursor()
                cur.execute(
                    "SELECT COUNT(*) FROM water_quality "
                    "WHERE determinand = %s AND value IS NOT NULL",
                    (ML_TARGET_DETERMINAND,)
                )
                count = cur.fetchone()[0]
                cur.close()
                conn.close()
                
                logger.debug("BOD rows in DB: %d", count)
                
                self._train_offline_model()

                # if count >= TRAIN_THRESHOLD:
                #     self._train_offline_model()
                # else:
                #     logger.info(
                #         "Not enough data for training: %d/%d",
                #         count, TRAIN_THRESHOLD
                #     )
                
            except Exception as e:
                logger.exception("Trainer error: %s", e)
            
            # Sleep with interruptibility
            for _ in range(TRAIN_CHECK_INTERVAL):
                if self._stop_event.is_set():
                    break
                time.sleep(1)
        
        logger.info("Background trainer stopped")
    
    def _train_offline_model(self) -> None:
        """Train RandomForest model on historical data."""
        import psycopg2
        
        try:
            conn = psycopg2.connect(CONN_STR)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT sample_time, value
                FROM water_quality
                WHERE determinand = %s AND value IS NOT NULL
                ORDER BY sample_time DESC
                LIMIT %s
                """,
                (ML_TARGET_DETERMINAND, TRAIN_FETCH_LIMIT)
            )
            rows = cur.fetchall()[::-1]  # Reverse to ascending
            cur.close()
            conn.close()
            
            if len(rows) < TRAIN_THRESHOLD:
                return
            
            # Build features
            times = [r[0] for r in rows]
            vals = [float(r[1]) for r in rows]
            prev_vals = [0.0] + vals[:-1]
            
            X = []
            y = []
            for t, v, pv in zip(times, vals, prev_vals):
                ts = t.replace(tzinfo=timezone.utc).timestamp()
                X.append([ts, pv, t.hour, t.weekday(), t.month])
                y.append(v)
            
            X = np.array(X)
            y = np.array(y)
            
            # Train
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            rf = RandomForestRegressor(n_estimators=200, n_jobs=-1, random_state=42)
            rf.fit(X_scaled, y)
            
            # Save models
            joblib.dump(rf, RF_MODEL_PATH)
            joblib.dump(scaler, SCALER_PATH)
            
            # Swap into production
            with self._model_lock:
                self._offline_model = rf
                self._offline_scaler = scaler
            
            logger.info("Trained RandomForest on %d samples", len(y))
            
        except Exception as e:
            logger.exception("Training failed: %s", e)
    
    # -------------------------
    # Message Processing
    # -------------------------
    def _parse_sample_time(self, data: Dict[str, Any]) -> Optional[datetime]:
        """Parse sample_time from enriched data."""
        ts_str = data.get("sample_time")
        if not ts_str:
            return None
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except:
            return None
    
    def process_message(self, msg) -> None:
        """Process a single message."""
        try:
            data = msg.value
            self.message_count += 1
            
            # Filter for BOD ATU only
            determinand = data.get("determinand")
            if determinand != ML_TARGET_DETERMINAND:
                return
            
            value = data.get("value")
            if value is None:
                return
            
            sample_time = self._parse_sample_time(data)
            if sample_time is None:
                return
            
            # Get prediction
            conn = psycopg2.connect(CONN_STR)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT value
                FROM water_quality
                WHERE determinand = %s
                AND value IS NOT NULL
                ORDER BY sample_time DESC
                LIMIT 1
                """,
                (ML_TARGET_DETERMINAND,)
            )
            row = cur.fetchone()
            prev_value = float(row[0]) if row else 0.0 
            cur.close()
            predicted, model_name = self._predict_with_best_model(sample_time, prev_value)
            
            self.prediction_count += 1
            
            # Build prediction message
            prediction_msg = {
                "id": data.get("id"),
                "sampling_point": data.get("sampling_point"),
                "sample_time": data.get("sample_time"),
                "determinand": determinand,
                "value": value,
                "predicted_value": predicted,
                "predicted_model": model_name,
                "features": {
                    "unix_time": sample_time.timestamp(),
                    "hour": sample_time.hour,
                    "day_of_week": sample_time.weekday(),
                    "month": sample_time.month,
                },
            }
            
            # Publish prediction
            if self.producer:
                self.producer.send(
                    self.output_topic,
                    key=data.get("sampling_point"),
                    value=prediction_msg,
                )
            
            # Write to database
            db_record = dict(data)
            db_record["predicted_value"] = predicted
            db_record["predicted_model"] = model_name
            
            if self.db_writer:
                self.db_writer.write(db_record)
            
            if self.prediction_count % 50 == 0:
                logger.info(
                    "Predictions: %d | Last: value=%.2f, pred=%.2f (%s)",
                    self.prediction_count, value,
                    predicted if predicted else 0.0, model_name
                )
                
        except Exception as e:
            logger.exception("Error processing message: %s", e)
    
    def run(self) -> None:
        """Main consumer loop."""
        logger.info("=" * 60)
        logger.info("ONLINE ML CONSUMER STARTING")
        logger.info("  Input:  %s", self.input_topic)
        logger.info("  Output: %s", self.output_topic)
        logger.info("  Target: %s", ML_TARGET_DETERMINAND)
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
    consumer = OnlineMLConsumer()
    consumer.run()


if __name__ == "__main__":
    main()
