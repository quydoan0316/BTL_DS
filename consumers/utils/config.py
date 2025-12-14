"""
Centralized configuration for all consumers.

All configuration is loaded from environment variables with sensible defaults.
"""

import os

# -----------------------------
# Kafka Configuration
# -----------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

# Kafka Topics
TOPIC_RAW = os.getenv("TOPIC_RAW", "water-quality-raw")
TOPIC_ENRICHED = os.getenv("TOPIC_ENRICHED", "water-quality-enriched")
TOPIC_PREDICTION = os.getenv("TOPIC_PREDICTION", "water-quality-prediction")
TOPIC_VIOLATION = os.getenv("TOPIC_VIOLATION", "water-quality-violation")
TOPIC_METRICS = os.getenv("TOPIC_METRICS", "water-quality-metrics")

# Consumer Group IDs
GROUP_PREPROCESS = os.getenv("GROUP_PREPROCESS", "preprocess-group")
GROUP_ML = os.getenv("GROUP_ML", "ml-group")
GROUP_VIOLATION = os.getenv("GROUP_VIOLATION", "violation-group")

# -----------------------------
# Database Configuration
# -----------------------------
# # Neon PostgreSQL connection string
# CONN_STR = os.getenv(
#     "CONN_STR",
#     "postgresql://postgres:admin@host.docker.internal:5433/hpb"
# )

# Local PostgreSQL connection string
CONN_STR = os.getenv(
    "CONN_STR",
    "postgresql://postgres:admin@host.docker.internal:5433/hpb"
)

# Database writer settings
DB_BATCH_SIZE = int(os.getenv("DB_BATCH_SIZE", "100"))
DB_QUEUE_SIZE = int(os.getenv("DB_QUEUE_SIZE", "20000"))

# -----------------------------
# UK Regulatory Limits
# -----------------------------
# Values in mg/L unless otherwise specified
UK_LIMITS = {
    "BOD ATU": 20.0,           # Biological Oxygen Demand
    "Ammonia (N)": 5.0,        # Ammonia as Nitrogen
    "Ammonia(N)": 5.0,         # Alternative naming
    "Oil": 10.0,               # Oil content
    "Oil & Grs Vs": 10.0,      # Oil and Grease Visual
    "Grs Vs": 10.0,            # Grease Visual
    "Lead - as Pb": 0.5,       # Lead
    "Cadmium - Cd": 0.005,     # Cadmium (very toxic)
    "Copper - Cu": 0.1,        # Copper
    "Zinc - as Zn": 0.5,       # Zinc
    "Nickel - Ni": 0.02,       # Nickel
    "Chromium -Cr": 0.05,      # Chromium
    "Sld Sus@105C": 60.0,      # Suspended Solids
}

# Violation severity thresholds
SEVERITY_WARNING = 0.8   # 80% of limit
SEVERITY_CRITICAL = 1.0  # At or above limit

# -----------------------------
# ML Configuration
# -----------------------------
MODEL_DIR = os.getenv("MODEL_DIR", "./models")
RF_MODEL_PATH = os.path.join(MODEL_DIR, "bod_rf.pkl")
SCALER_PATH = os.path.join(MODEL_DIR, "bod_scaler.pkl")

# Training settings
TRAIN_CHECK_INTERVAL = int(os.getenv("TRAIN_CHECK_INTERVAL", "60"))
TRAIN_THRESHOLD = int(os.getenv("TRAIN_THRESHOLD", "500"))
TRAIN_FETCH_LIMIT = int(os.getenv("TRAIN_FETCH_LIMIT", "50000"))

# Target determinand for ML predictions
ML_TARGET_DETERMINAND = "BOD ATU"

# -----------------------------
# Grid Configuration
# -----------------------------
GRID_SIZE = 10000  # For region determination from coordinates

# -----------------------------
# Utility Functions
# -----------------------------
def get_limit(determinand: str) -> float:
    """Get the UK regulatory limit for a determinand."""
    return UK_LIMITS.get(determinand)


def get_severity_level(value: float, limit: float) -> str:
    """
    Determine severity level based on value vs limit.
    
    Returns:
        "none", "warning", or "critical"
    """
    if limit is None or limit <= 0:
        return "none"
    
    ratio = value / limit
    
    if ratio >= SEVERITY_CRITICAL:
        return "critical"
    elif ratio >= SEVERITY_WARNING:
        return "warning"
    else:
        return "none"
