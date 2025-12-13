#!/usr/bin/env python3
"""
Realtime Kafka -> Neon Postgres consumer with:
 - Online learning (SGDRegressor) for immediate predictions
 - Background periodic offline retrainer (RandomForest) to improve accuracy
 - Robust DB writer (batch + reconnect)
 - Writes predicted_value & predicted_model into water_quality table
 - Designed for BOD ATU forecasting but structured to extend for other determinands
"""

import os
import json
import time
import math
import logging
import threading
import queue
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import Json, register_default_json, register_default_jsonb, execute_values
from psycopg2 import OperationalError, InterfaceError, DatabaseError

# ML libs
import numpy as np
from sklearn.linear_model import SGDRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import joblib

# -----------------------------
# CONFIG - chỉnh nếu cần
# -----------------------------
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "water-quality")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
# Neon connection string (yours)
CONN_STR = os.getenv("CONN_STR", "postgresql://neondb_owner:npg_vSVkYdKPj6Q2@ep-silent-water-a1q1b7so-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

BATCH_SIZE = 100
DB_QUEUE = queue.Queue(maxsize=20000)
STOP_EVENT = threading.Event()

# retrain settings
TRAIN_CHECK_INTERVAL = 60            # seconds between checks for retrain
TRAIN_THRESHOLD = 500                # minimum number of BOD rows to run offline retrain
TRAIN_FETCH_LIMIT = 50000            # max rows to fetch for training
MODEL_DIR = os.path.join(os.getcwd(), "models")
os.makedirs(MODEL_DIR, exist_ok=True)
RF_MODEL_PATH = os.path.join(MODEL_DIR, "bod_rf.pkl")
SCALER_PATH = os.path.join(MODEL_DIR, "bod_scaler.pkl")

# UK regulatory limits
LIMITS = {
    "BOD ATU": 20.0,
    "Ammonia (N)": 5.0,
    "Oil": 10.0,
    "Grs Vs": 10.0,
    "Lead - as Pb": 0.5,
}

GRID_SIZE = 10000

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("water-consumer-ml")

def osgb36_to_wgs84(easting, northing):
    try:
        E = float(easting); N = float(northing)
    except Exception:
        return None, None
    a = 6377563.396; b = 6356256.909; F0 = 0.9996012717
    lat0 = math.radians(49); lon0 = math.radians(-2)
    N0 = -100000; E0 = 400000
    e2 = 1 - (b*b)/(a*a); n = (a-b)/(a+b)
    lat = lat0; M = 0
    while True:
        lat_prev = lat
        M = b*F0*((1+n+5/4*n*n+5/4*n*n*n)*(lat-lat0) 
                  - (3*n+3*n*n+21/8*n*n*n)*math.sin(lat-lat0)*math.cos(lat+lat0) 
                  + (15/8*n*n+15/8*n*n*n)*math.sin(2*(lat-lat0))*math.cos(2*(lat+lat0)) 
                  - (35/24*n*n*n)*math.sin(3*(lat-lat0))*math.cos(3*(lat+lat0)))
        lat = (N - N0 - M)/(a*F0) + lat
        if abs(lat - lat_prev) < 1e-10:
            break
    nu = a*F0/math.sqrt(1 - e2*(math.sin(lat)**2))
    rho = a*F0*(1-e2)/((1 - e2*(math.sin(lat)**2))**1.5)
    eta2 = nu/rho - 1
    sec_lat = 1/math.cos(lat)
    dE = E - E0
    VII = math.tan(lat)/(2*rho*nu)
    VIII = math.tan(lat)/(24*rho*nu**3)*(5+3*math.tan(lat)**2+eta2-9*math.tan(lat)**2*eta2)
    IX = math.tan(lat)/(720*rho*nu**5)*(61+90*math.tan(lat)**2+45*math.tan(lat)**4)
    X = sec_lat/nu
    XI = sec_lat/(6*nu**3)*(nu/rho+2*math.tan(lat)**2)
    XII = sec_lat/(120*nu**5)*(5+28*math.tan(lat)**2+24*math.tan(lat)**4)
    XIIA = sec_lat/(5040*nu**7)*(61+662*math.tan(lat)**2+1320*math.tan(lat)**4+720*math.tan(lat)**6)
    lat_ = lat - VII*dE**2 + VIII*dE**4 - IX*dE**6
    lon_ = lon0 + X*dE - XI*dE**3 + XII*dE**5 - XIIA*dE**7
    return math.degrees(lat_), math.degrees(lon_)

# -----------------------------
# Helpers
# -----------------------------
def safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s.replace(",", ""))
    except:
        return None

def determine_region_from_coords(e, n):
    try:
        e = int(float(e))
        n = int(float(n))
        return f"E{(e//GRID_SIZE)*GRID_SIZE}_N{(n//GRID_SIZE)*GRID_SIZE}"
    except:
        return None

def build_violation_details(det, val):
    if det not in LIMITS or val is None:
        return None
    limit = LIMITS[det]
    sev = val / limit if limit > 0 else None
    return {"value": val, "limit": limit, "severity": sev, "is_violation": sev > 1 if sev else False}

# -----------------------------
# DB table create/ensure
# -----------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS water_quality (
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
    inserted_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wq_time ON water_quality(sample_time);
CREATE INDEX IF NOT EXISTS idx_wq_region ON water_quality(region);
CREATE INDEX IF NOT EXISTS idx_wq_det ON water_quality(determinand);
CREATE INDEX IF NOT EXISTS idx_wq_violation ON water_quality(violation);
"""

def ensure_table(conn):
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    cur.close()
    logger.info("Ensured table water_quality exists (with prediction columns).")

# -----------------------------
# ML: Online model (SGD) + background RandomForest
# -----------------------------
# online model state
_online_lock = threading.Lock()
_online_model = SGDRegressor(max_iter=1000, tol=1e-3)
_online_initialized = False
_welford_count = 0
_welford_mean = 0.0
_welford_M2 = 0.0  # for variance

# current best offline model (RandomForest) swapped when available
_model_lock = threading.Lock()
_offline_model = None    # RandomForestRegressor instance when trained
_offline_scaler = None   # StandardScaler for offline features

def _welford_update(x: float):
    global _welford_count, _welford_mean, _welford_M2
    _welford_count += 1
    if _welford_count == 1:
        _welford_mean = x
        _welford_M2 = 0.0
    else:
        delta = x - _welford_mean
        _welford_mean += delta / _welford_count
        delta2 = x - _welford_mean
        _welford_M2 += delta * delta2

def _welford_std():
    if _welford_count < 2:
        return 1.0
    return math.sqrt(_welford_M2 / (_welford_count - 1))

def online_update_and_predict(sample_time: datetime, value: float) -> float:
    """
    Update online SGD model with a single sample and return prediction (on same sample).
    Feature used: normalized unix_time (and later we'll extend with prev_value if available).
    """
    global _online_initialized, _online_model
    ts = sample_time.replace(tzinfo=timezone.utc).timestamp()
    # update running mean/std
    _welford_update(ts)
    std = max(_welford_std(), 1e-6)
    X = np.array([[ (ts - _welford_mean) / std ]], dtype=float)
    y = np.array([value], dtype=float)
    # partial fit; for regression need to give shape (n_samples, n_features)
    try:
        if not _online_initialized:
            _online_model.partial_fit(X, y)
            _online_initialized = True
        else:
            _online_model.partial_fit(X, y)
        pred = float(_online_model.predict(X)[0])
    except Exception as e:
        logger.exception("Online model update/predict error: %s", e)
        pred = None
    return pred

def predict_with_current_model(sample_time: Optional[datetime], prev_value: Optional[float]) -> Tuple[Optional[float], str]:
    """
    Make prediction using best available model. Return (predicted_value, model_name).
    Priority: offline_model (if available) else online_model.
    Features for offline RF: [unix_time, prev_value, hour, dayofweek, month]
    For online_model (SGD) we use only normalized unix_time.
    """
    if sample_time is None:
        return None, "none"
    ts = sample_time.replace(tzinfo=timezone.utc).timestamp()
    # try offline
    with _model_lock:
        if _offline_model is not None and _offline_scaler is not None:
            try:
                hour = sample_time.hour
                dow = sample_time.weekday()
                month = sample_time.month
                prev = prev_value if prev_value is not None else 0.0
                X_raw = np.array([[ts, prev, hour, dow, month]], dtype=float)
                X_scaled = _offline_scaler.transform(X_raw)
                pred = float(_offline_model.predict(X_scaled)[0])
                return pred, "random_forest"
            except Exception as e:
                logger.exception("Offline model prediction failed: %s", e)
                # fallback to online
    # fallback online
    try:
        pred = online_update_and_predict(sample_time, prev_value if prev_value is not None else 0.0)
        return pred, "online_sgd"
    except Exception as e:
        logger.exception("Online prediction fallback failed: %s", e)
        return None, "none"

# -----------------------------
# DB connect + writer thread
# -----------------------------
def db_connect():
    while not STOP_EVENT.is_set():
        try:
            conn = psycopg2.connect(CONN_STR)
            conn.autocommit = False
            return conn
        except Exception as e:
            logger.warning("DB connect failed, retrying in 2s: %s", e)
            time.sleep(2)
    raise RuntimeError("STOP_EVENT set while trying to connect to DB")

def db_worker():
    conn = None
    while not STOP_EVENT.is_set():
        batch = []
        try:
            item = DB_QUEUE.get(timeout=1)
            batch.append(item)
            while len(batch) < BATCH_SIZE:
                try:
                    batch.append(DB_QUEUE.get_nowait())
                except queue.Empty:
                    break

            if conn is None or conn.closed:
                conn = db_connect()
                ensure_table(conn)

            sql = """
            INSERT INTO water_quality
            (id, sampling_point, sample_time, determinand, value, unit,
             violation, violations, easting, northing, latitude, longitude,
             region, raw_data, predicted_value, predicted_model)
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET
               sampling_point = EXCLUDED.sampling_point,
               sample_time = EXCLUDED.sample_time,
               determinand = EXCLUDED.determinand,
               value = EXCLUDED.value,
               unit = EXCLUDED.unit,
               violation = EXCLUDED.violation,
               violations = EXCLUDED.violations,
               easting = EXCLUDED.easting,
               northing = EXCLUDED.northing,
               latitude = EXCLUDED.latitude,
               longitude = EXCLUDED.longitude,
               region = EXCLUDED.region,
               raw_data = EXCLUDED.raw_data,
               predicted_value = EXCLUDED.predicted_value,
               predicted_model = EXCLUDED.predicted_model;
            """

            params = [
                (
                    r["id"], r["sampling_point"], r["sample_time"],
                    r["determinand"], r["value"], r["unit"],
                    r["violation"], Json(r["violations"]),
                    r["easting"], r["northing"], r["latitude"], r["longitude"],
                    r["region"], Json(r["raw_data"]),
                    r.get("predicted_value"), r.get("predicted_model")
                )
                for r in batch
            ]

            cur = conn.cursor()
            execute_values(cur, sql, params, template=None, page_size=100)
            conn.commit()
            cur.close()
            logger.info("DB INSERT OK: %d rows (last determinand=%s)", len(batch), batch[-1].get("determinand"))
        except (OperationalError, InterfaceError, DatabaseError) as e:
            logger.exception("DB error: %s – reconnecting...", e)
            if conn:
                try:
                    conn.close()
                except:
                    pass
            conn = None
            time.sleep(1)
        except queue.Empty:
            continue
        except Exception as e:
            logger.exception("DB worker fatal error: %s", e)
            time.sleep(1)
    # flush remaining items
    while not DB_QUEUE.empty():
        try:
            # best-effort flush
            item = DB_QUEUE.get_nowait()
            # try simple insert with fresh conn
            conn = db_connect()
            ensure_table(conn)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO water_quality (id, sampling_point, sample_time, determinand, value, predicted_value, predicted_model, raw_data)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (item["id"], item["sampling_point"], item["sample_time"], item["determinand"], item["value"], item.get("predicted_value"), item.get("predicted_model"), Json(item["raw_data"])))
            conn.commit()
            cur.close()
        except Exception:
            break

# -----------------------------
# Background trainer thread
# -----------------------------
def fetch_bod_rows_for_training(conn, limit=TRAIN_FETCH_LIMIT):
    """
    Fetch last N BOD ATU rows (sample_time, value) ordered by sample_time asc.
    Returns DataFrame-like lists.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT sample_time, value, COALESCE((raw_data->>'@id'), id) as id
        FROM water_quality
        WHERE determinand = 'BOD ATU' AND value IS NOT NULL
        ORDER BY sample_time DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    # reverse to ascending time
    rows = rows[::-1]
    return rows

def background_trainer():
    """
    Periodically checks DB for sufficient BOD ATU data. If >= TRAIN_THRESHOLD,
    fetches recent rows and trains RandomForest offline, saves model + scaler,
    and swaps them into use.
    """
    global _offline_model, _offline_scaler
    while not STOP_EVENT.is_set():
        try:
            conn = db_connect()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM water_quality WHERE determinand = 'BOD ATU' AND value IS NOT NULL;")
            cnt = cur.fetchone()[0]
            cur.close()
            conn.close()
            logger.debug("BOD rows in DB = %d", cnt)
            if cnt >= TRAIN_THRESHOLD:
                # fetch data
                conn = db_connect()
                rows = fetch_bod_rows_for_training(conn, limit=TRAIN_FETCH_LIMIT)
                conn.close()
                if len(rows) < TRAIN_THRESHOLD:
                    logger.info("Not enough rows after fetch: %d", len(rows))
                else:
                    # build feature matrix: unix_time, prev_value, hour, dow, month
                    times = []
                    vals = []
                    for r in rows:
                        times.append(r[0])
                        vals.append(float(r[1]))

                    # compute prev_value (lag1). we align prev_value to current sample by shifting
                    prev_vals = [0.0] + vals[:-1]

                    X = []
                    y = []
                    for t, v, pv in zip(times, vals, prev_vals):
                        ts = t.replace(tzinfo=timezone.utc).timestamp()
                        hour = t.hour
                        dow = t.weekday()
                        month = t.month
                        X.append([ts, pv, hour, dow, month])
                        y.append(v)
                    X = np.array(X, dtype=float)
                    y = np.array(y, dtype=float)

                    # scaler + random forest training
                    scaler = StandardScaler()
                    Xs = scaler.fit_transform(X)
                    rf = RandomForestRegressor(n_estimators=200, n_jobs=-1, random_state=42)
                    rf.fit(Xs, y)

                    # save to disk
                    joblib.dump(rf, RF_MODEL_PATH)
                    joblib.dump(scaler, SCALER_PATH)
                    logger.info("Background trainer: trained RandomForest on %d samples and saved model.", len(y))

                    # swap into production
                    with _model_lock:
                        _offline_model = rf
                        _offline_scaler = scaler
                    logger.info("Background trainer: swapped offline model into serving.")

                    # sleep longer after successful training
                    time.sleep(60)
            else:
                # not enough rows yet
                logger.info("Background trainer: not enough BOD rows (%d/%d).", cnt, TRAIN_THRESHOLD)
            # sleep until next check
            for _ in range(int(TRAIN_CHECK_INTERVAL)):
                if STOP_EVENT.is_set():
                    break
                time.sleep(1)
        except Exception as e:
            logger.exception("Background trainer error: %s", e)
            time.sleep(5)

# -----------------------------
# Parse Kafka -> row
# -----------------------------
def parse_message(msg) -> Dict[str, Any]:
    d = msg.value
    det = d.get("determinand.label") or d.get("determinand")
    val = safe_float(d.get("result"))
    E = d.get("sample.samplingPoint.easting") or d.get("easting")
    N = d.get("sample.samplingPoint.northing") or d.get("northing")

    lat, lon = (None, None)
    if E and N:
        try:
            lat, lon = osgb36_to_wgs84(E, N)
        except:
            lat, lon = (None, None)

    sp = d.get("sample.samplingPoint.label") or d.get("sampling_point")
    region = sp if sp else (determine_region_from_coords(E, N) if E and N else None)

    vinfo = build_violation_details(det, val)
    viol_bool = vinfo["is_violation"] if vinfo else None

    ts_raw = d.get("sample.sampleDateTime") or d.get("sampleDateTime")
    sample_time = None
    if ts_raw:
        try:
            sample_time = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except:
            try:
                sample_time = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
            except:
                sample_time = None

    # find previous value for same station/determinant if possible (best-effort: query DB synchronously)
    prev_value = None
    if sample_time is not None and (sp or region) and det == "BOD ATU":
        try:
            conn = db_connect()
            cur = conn.cursor()
            # find last value before current sample_time for same sampling_point if exists
            if sp:
                cur.execute("""
                    SELECT value FROM water_quality
                    WHERE sampling_point = %s AND determinand = 'BOD ATU' AND sample_time < %s AND value IS NOT NULL
                    ORDER BY sample_time DESC LIMIT 1
                """, (sp, sample_time))
            else:
                # fallback by region
                cur.execute("""
                    SELECT value FROM water_quality
                    WHERE region = %s AND determinand = 'BOD ATU' AND sample_time < %s AND value IS NOT NULL
                    ORDER BY sample_time DESC LIMIT 1
                """, (region, sample_time))
            row = cur.fetchone()
            cur.close()
            conn.close()
            if row:
                prev_value = float(row[0])
        except Exception:
            # ignore DB read failures here (best-effort)
            prev_value = None

    # prediction: only for BOD ATU in this implementation
    predicted_value = None
    predicted_model = None
    if det == "BOD ATU" and sample_time is not None:
        try:
            pred, model_name = predict_with_current_model(sample_time, prev_value)
            predicted_value = pred
            predicted_model = model_name
        except Exception as e:
            logger.exception("Prediction error: %s", e)
            predicted_value = None
            predicted_model = None

    row = {
        "id": d.get("@id") or None,
        "sampling_point": sp,
        "sample_time": sample_time,
        "determinand": det,
        "value": val,
        "unit": d.get("determinand.unit.label") or d.get("unit"),
        "violation": viol_bool,
        "violations": {det: vinfo} if vinfo else None,
        "easting": int(float(E)) if E else None,
        "northing": int(float(N)) if N else None,
        "latitude": lat,
        "longitude": lon,
        "region": region,
        "raw_data": d,
        "predicted_value": predicted_value,
        "predicted_model": predicted_model
    }
    return row

# -----------------------------
# Main consumer loop
# -----------------------------
def start_consumer():
    # start DB writer
    threading.Thread(target=db_worker, daemon=True).start()
    # start background trainer
    threading.Thread(target=background_trainer, daemon=True).start()

    logger.info("Starting Kafka consumer... (topic=%s)", KAFKA_TOPIC)
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="wq-ml-writer",
        max_poll_interval_ms=300000,
        session_timeout_ms=10000
    )

    try:
        for msg in consumer:
            row = parse_message(msg)
            try:
                DB_QUEUE.put_nowait(row)
            except queue.Full:
                logger.warning("DB queue full -> dropping message id=%s", row.get("id"))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception("Consumer fatal error: %s", e)
    finally:
        STOP_EVENT.set()
        consumer.close()
        logger.info("Consumer stopped.")

if __name__ == "__main__":
    start_consumer()
