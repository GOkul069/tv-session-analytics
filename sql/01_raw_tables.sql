-- Snowflake: landing tables for raw set-top box telemetry and reference data.
-- Raw files arrive as gzip CSV in an external stage (S3) partitioned by event date.

CREATE SCHEMA IF NOT EXISTS ANALYTICS.RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.CURATED;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.MARTS;

CREATE FILE FORMAT IF NOT EXISTS ANALYTICS.RAW.CSV_GZ
    TYPE = CSV COMPRESSION = GZIP SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = TRUE
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS';

CREATE STAGE IF NOT EXISTS ANALYTICS.RAW.STB_STAGE
    URL = 's3://iptv-telemetry/raw/'
    STORAGE_INTEGRATION = S3_TELEMETRY_INT
    FILE_FORMAT = ANALYTICS.RAW.CSV_GZ;

CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.STB_EVENTS (
    device_id        VARCHAR      NOT NULL,
    event_time       TIMESTAMP_NTZ NOT NULL,
    event_type       VARCHAR      NOT NULL,   -- TUNE | STANDBY
    channel_id       VARCHAR,                 -- NULL for STANDBY and VOD
    playback_mode    VARCHAR,                 -- LIVE | STARTOVER | CATCHUP | CLOUD_DVR | VOD
    content_age_min  INTEGER,                 -- minutes behind original broadcast (time-shifted modes)
    _loaded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file     VARCHAR
)
CLUSTER BY (TO_DATE(event_time));

COPY INTO ANALYTICS.RAW.STB_EVENTS (device_id, event_time, event_type, channel_id, playback_mode,
                                    content_age_min, _source_file)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME FROM @ANALYTICS.RAW.STB_STAGE/stb_events/)
ON_ERROR = 'CONTINUE';

CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.CHANNELS (
    channel_id VARCHAR PRIMARY KEY, channel_name VARCHAR, genre VARCHAR, is_hd BOOLEAN,
    startover_enabled BOOLEAN, catchup_window_days INTEGER
);
CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.DEVICES (
    device_id VARCHAR PRIMARY KEY, region VARCHAR, box_model VARCHAR, package VARCHAR,
    activation_date DATE
);
CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.MAINTENANCE_EVENTS (
    maintenance_id VARCHAR PRIMARY KEY, channel_id VARCHAR, start_time TIMESTAMP_NTZ,
    duration_min INTEGER, maintenance_type VARCHAR, is_planned BOOLEAN, deferrable BOOLEAN
);
CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.STORAGE_USAGE_DAILY (
    usage_date DATE, channel_id VARCHAR, feature VARCHAR, stored_gb NUMBER(14, 1), storage_tier VARCHAR
);
CREATE TABLE IF NOT EXISTS ANALYTICS.RAW.STORAGE_RATE_CARD (
    storage_tier VARCHAR PRIMARY KEY, usd_per_gb_month NUMBER(10, 4), description VARCHAR
);
