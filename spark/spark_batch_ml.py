#!/usr/bin/env python3
"""
Spark Batch ML Training for Water Quality Prediction.

This job:
1. Reads historical data from PostgreSQL
2. Trains RandomForest model using Spark MLlib
3. Evaluates model (MAE, RMSE, R²)
4. Saves model for later use

Run periodically (e.g., daily) or when sufficient new data is available.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, unix_timestamp, hour, dayofweek, month, lag, when
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# -------------------------
# Configuration
# -------------------------
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5433/hpb_spark")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin")
MODEL_PATH = os.getenv("MODEL_PATH", "/app/models/rf_bod_model")
MIN_TRAINING_ROWS = int(os.getenv("MIN_TRAINING_ROWS", "500"))

# -------------------------
# Spark Session
# -------------------------
spark = SparkSession.builder \
    .appName("WaterQualityBatchML") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


def load_training_data():
    """Load BOD ATU data from PostgreSQL for training."""
    print("[ML] Loading training data from PostgreSQL...")
    
    df = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "(SELECT * FROM water_quality WHERE determinand = 'BOD ATU' AND value IS NOT NULL ORDER BY sample_time) AS bod_data") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    print(f"[ML] Loaded {df.count()} BOD ATU records")
    return df


def prepare_features(df):
    """Prepare features for ML training."""
    print("[ML] Preparing features...")
    
    # Add time-based features
    df = df.withColumn("unix_time", unix_timestamp(col("sample_time")))
    df = df.withColumn("hour", hour(col("sample_time")))
    df = df.withColumn("day_of_week", dayofweek(col("sample_time")))
    df = df.withColumn("month", month(col("sample_time")))
    
    # Add lag feature (previous value)
    window_spec = Window.orderBy("sample_time")
    df = df.withColumn("prev_value", lag("value", 1).over(window_spec))
    
    # Fill null prev_value with current value
    df = df.withColumn(
        "prev_value",
        when(col("prev_value").isNull(), col("value")).otherwise(col("prev_value"))
    )
    
    # Filter out rows with null features
    df = df.filter(
        col("unix_time").isNotNull() &
        col("hour").isNotNull() &
        col("day_of_week").isNotNull() &
        col("month").isNotNull() &
        col("prev_value").isNotNull() &
        col("value").isNotNull()
    )
    
    return df


def train_random_forest(df):
    """Train RandomForest model."""
    print("[ML] Training RandomForest model...")
    
    # Feature columns
    feature_cols = ["unix_time", "prev_value", "hour", "day_of_week", "month"]
    
    # Create feature vector
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_raw"
    )
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True
    )
    
    # RandomForest model
    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="value",
        numTrees=200,
        maxDepth=10,
        seed=42
    )
    
    # Build pipeline
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    
    # Split data
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"[ML] Training set: {train_df.count()}, Test set: {test_df.count()}")
    
    # Train
    model = pipeline.fit(train_df)
    
    return model, test_df


def evaluate_model(model, test_df):
    """Evaluate model performance."""
    print("[ML] Evaluating model...")
    
    predictions = model.transform(test_df)
    
    # Evaluators
    mae_evaluator = RegressionEvaluator(
        labelCol="value",
        predictionCol="prediction",
        metricName="mae"
    )
    rmse_evaluator = RegressionEvaluator(
        labelCol="value",
        predictionCol="prediction",
        metricName="rmse"
    )
    r2_evaluator = RegressionEvaluator(
        labelCol="value",
        predictionCol="prediction",
        metricName="r2"
    )
    
    mae = mae_evaluator.evaluate(predictions)
    rmse = rmse_evaluator.evaluate(predictions)
    r2 = r2_evaluator.evaluate(predictions)
    
    print("=" * 50)
    print("MODEL EVALUATION RESULTS")
    print("=" * 50)
    print(f"  MAE  (Mean Absolute Error): {mae:.4f}")
    print(f"  RMSE (Root Mean Square Error): {rmse:.4f}")
    print(f"  R²   (Coefficient of Determination): {r2:.4f}")
    print("=" * 50)
    
    return {"mae": mae, "rmse": rmse, "r2": r2}


def save_model(model, path):
    """Save trained model."""
    print(f"[ML] Saving model to {path}...")
    model.write().overwrite().save(path)
    print("[ML] Model saved successfully!")


def main():
    """Main training pipeline."""
    print("=" * 60)
    print("SPARK ML BATCH TRAINING - BOD ATU Prediction")
    print("=" * 60)
    
    # Load data
    df = load_training_data()
    
    if df.count() < MIN_TRAINING_ROWS:
        print(f"[ML] Not enough data for training. Need at least {MIN_TRAINING_ROWS} rows.")
        return
    
    # Prepare features
    df = prepare_features(df)
    
    # Train model
    model, test_df = train_random_forest(df)
    
    # Evaluate
    metrics = evaluate_model(model, test_df)
    
    # Save model
    save_model(model, MODEL_PATH)
    
    print("\n[ML] Training complete!")
    print(f"  Model saved to: {MODEL_PATH}")
    print(f"  Metrics: MAE={metrics['mae']:.4f}, RMSE={metrics['rmse']:.4f}, R²={metrics['r2']:.4f}")


if __name__ == "__main__":
    main()
