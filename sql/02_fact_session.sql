-- Snowflake: sessionize raw telemetry into CURATED.FACT_SESSION.
-- Same rules as src/spark_sessionize.py and src/sessionize.py:
--   * drop exact duplicates and events outside the extraction window
--   * a session starts at each TUNE and ends at the device's next event,
--     capped at a 240-minute inactivity timeout
--   * sessions shorter than 30 seconds are flagged as zaps
-- Incremental pattern: re-process a rolling window of recent dates and MERGE on session_id.

SET window_start = '2025-03-03';
SET window_end   = '2025-04-01';

CREATE OR REPLACE TRANSIENT TABLE ANALYTICS.CURATED.STG_EVENTS_CLEAN AS
SELECT DISTINCT device_id, event_time, event_type, channel_id, playback_mode, content_age_min
FROM ANALYTICS.RAW.STB_EVENTS
WHERE event_time >= $window_start::TIMESTAMP_NTZ
  AND event_time <  $window_end::TIMESTAMP_NTZ;

CREATE TABLE IF NOT EXISTS ANALYTICS.CURATED.FACT_SESSION (
    session_id      NUMBER,
    device_id       VARCHAR,
    session_start   TIMESTAMP_NTZ,
    session_end     TIMESTAMP_NTZ,
    session_date    DATE,
    start_hour      SMALLINT,
    channel_id      VARCHAR,
    playback_mode   VARCHAR,
    content_age_min INTEGER,
    duration_sec    INTEGER,
    is_zap          BOOLEAN,
    end_reason      VARCHAR
)
CLUSTER BY (session_date, channel_id);

MERGE INTO ANALYTICS.CURATED.FACT_SESSION t
USING (
    WITH ordered AS (
        SELECT e.*,
               LEAD(event_time) OVER (PARTITION BY device_id ORDER BY event_time, event_type) AS next_time,
               LEAD(event_type) OVER (PARTITION BY device_id ORDER BY event_time, event_type) AS next_type
        FROM ANALYTICS.CURATED.STG_EVENTS_CLEAN e
    ),
    tunes AS (
        SELECT device_id, event_time AS session_start, channel_id, playback_mode, content_age_min,
               next_time, next_type,
               DATEADD(minute, 240, event_time) AS cap_time
        FROM ordered
        WHERE event_type = 'TUNE'
    )
    SELECT HASH(device_id, session_start, channel_id, playback_mode)        AS session_id,
           device_id,
           session_start,
           IFF(next_time IS NULL OR next_time > cap_time, cap_time, next_time) AS session_end,
           TO_DATE(session_start)                                          AS session_date,
           HOUR(session_start)                                             AS start_hour,
           channel_id, playback_mode, content_age_min,
           DATEDIFF(second, session_start,
                    IFF(next_time IS NULL OR next_time > cap_time, cap_time, next_time)) AS duration_sec,
           duration_sec < 30                                               AS is_zap,
           CASE WHEN next_time IS NULL OR next_time > cap_time THEN 'TIMEOUT'
                WHEN next_type = 'STANDBY' THEN 'STANDBY'
                ELSE 'NEXT_TUNE' END                                       AS end_reason
    FROM tunes
) s
ON t.session_id = s.session_id
WHEN MATCHED THEN UPDATE SET
    session_end = s.session_end, duration_sec = s.duration_sec, is_zap = s.is_zap, end_reason = s.end_reason
WHEN NOT MATCHED THEN INSERT VALUES (
    s.session_id, s.device_id, s.session_start, s.session_end, s.session_date, s.start_hour,
    s.channel_id, s.playback_mode, s.content_age_min, s.duration_sec, s.is_zap, s.end_reason);

-- Data quality assertions (fail the task if any returns rows)
SELECT 'negative duration' AS issue, COUNT(*) FROM ANALYTICS.CURATED.FACT_SESSION WHERE duration_sec < 0 HAVING COUNT(*) > 0
UNION ALL
SELECT 'duplicate session_id', COUNT(*) FROM (
    SELECT session_id FROM ANALYTICS.CURATED.FACT_SESSION GROUP BY 1 HAVING COUNT(*) > 1) HAVING COUNT(*) > 0;
