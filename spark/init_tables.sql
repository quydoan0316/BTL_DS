-- SQL init script for Spark output tables
-- Run this on your PostgreSQL database before starting Spark jobs

-- Table for processed records from Spark Streaming
CREATE TABLE IF NOT EXISTS water_quality_spark (
    id TEXT,
    sampling_point TEXT,
    sample_time TIMESTAMP,
    determinand TEXT,
    value DOUBLE PRECISION,
    unit TEXT,
    easting DOUBLE PRECISION,
    northing DOUBLE PRECISION,
    hour INTEGER,
    day_of_week INTEGER,
    month INTEGER,
    violation BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, processed_at)
);

-- Table for hourly aggregations
CREATE TABLE IF NOT EXISTS water_quality_hourly_agg (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    determinand TEXT,
    record_count BIGINT,
    avg_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    violation_count BIGINT,
    PRIMARY KEY (window_start, determinand)
);

-- Indexes for faster Grafana queries
CREATE INDEX IF NOT EXISTS idx_spark_sample_time ON water_quality_spark(sample_time);
CREATE INDEX IF NOT EXISTS idx_spark_determinand ON water_quality_spark(determinand);
CREATE INDEX IF NOT EXISTS idx_agg_window ON water_quality_hourly_agg(window_start);
CREATE INDEX IF NOT EXISTS idx_agg_determinand ON water_quality_hourly_agg(determinand);
