-- Snowflake: dashboard marts built on CURATED.FACT_SESSION.
-- Refreshed daily by a Snowflake task; the BI dashboards read only from MARTS.

-- Hourly viewing by channel: sessions, unique devices and viewer-minutes per hour bucket.
-- Sessions are split across the hour buckets they span so viewing time lands in the right hour.
CREATE OR REPLACE TABLE ANALYTICS.MARTS.AGG_HOURLY_CHANNEL_VIEWING AS
WITH hours AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS k FROM TABLE(GENERATOR(ROWCOUNT => 5))
),
split AS (
    SELECT f.session_id, f.device_id, f.channel_id, f.playback_mode,
           DATEADD(hour, h.k, DATE_TRUNC('hour', f.session_start)) AS hour_start,
           GREATEST(f.session_start, DATEADD(hour, h.k, DATE_TRUNC('hour', f.session_start))) AS seg_start,
           LEAST(f.session_end, DATEADD(hour, h.k + 1, DATE_TRUNC('hour', f.session_start))) AS seg_end
    FROM ANALYTICS.CURATED.FACT_SESSION f
    JOIN hours h ON DATEADD(hour, h.k, DATE_TRUNC('hour', f.session_start)) < f.session_end
    WHERE NOT f.is_zap
)
SELECT TO_DATE(hour_start) AS view_date, HOUR(hour_start) AS view_hour, channel_id, playback_mode,
       COUNT(DISTINCT session_id) AS sessions,
       COUNT(DISTINCT device_id)  AS unique_devices,
       SUM(DATEDIFF(second, seg_start, seg_end)) / 60.0 AS viewer_minutes
FROM split
GROUP BY 1, 2, 3, 4;

-- Maintenance impact: sessions overlapping a channel maintenance window (interval join).
CREATE OR REPLACE TABLE ANALYTICS.MARTS.FACT_MAINTENANCE_IMPACT AS
SELECT m.maintenance_id, m.channel_id, m.start_time, m.maintenance_type, m.is_planned, m.deferrable,
       HOUR(m.start_time) BETWEEN 18 AND 21 AS is_peak,
       f.session_id, f.device_id,
       DATEDIFF(second, GREATEST(f.session_start, m.start_time),
                LEAST(f.session_end, DATEADD(minute, m.duration_min, m.start_time))) / 60.0 AS disrupted_minutes
FROM ANALYTICS.RAW.MAINTENANCE_EVENTS m
JOIN ANALYTICS.CURATED.FACT_SESSION f
  ON f.channel_id = m.channel_id
 AND f.session_start < DATEADD(minute, m.duration_min, m.start_time)
 AND f.session_end   > m.start_time
WHERE NOT f.is_zap
  AND f.playback_mode IN ('LIVE', 'STARTOVER');   -- modes served from the live origin

-- Weekly share of active devices hit by at least one maintenance window.
CREATE OR REPLACE VIEW ANALYTICS.MARTS.V_WEEKLY_MAINTENANCE_REACH AS
WITH active AS (
    SELECT DATE_TRUNC('week', session_date) AS wk, COUNT(DISTINCT device_id) AS active_devices
    FROM ANALYTICS.CURATED.FACT_SESSION GROUP BY 1
),
hit AS (
    SELECT DATE_TRUNC('week', TO_DATE(start_time)) AS wk,
           COUNT(DISTINCT device_id) AS affected_devices,
           COUNT(DISTINCT IFF(is_peak, device_id, NULL)) AS affected_devices_peak
    FROM ANALYTICS.MARTS.FACT_MAINTENANCE_IMPACT GROUP BY 1
)
SELECT a.wk, a.active_devices, h.affected_devices, h.affected_devices_peak,
       h.affected_devices / a.active_devices AS affected_share
FROM active a LEFT JOIN hit h USING (wk);

-- Storage cost attribution by feature, with session share for comparison.
CREATE OR REPLACE VIEW ANALYTICS.MARTS.V_STORAGE_COST_BY_FEATURE AS
WITH cost AS (
    SELECT u.feature, SUM(u.stored_gb * r.usd_per_gb_month / 30) AS cost_usd
    FROM ANALYTICS.RAW.STORAGE_USAGE_DAILY u
    JOIN ANALYTICS.RAW.STORAGE_RATE_CARD r USING (storage_tier)
    GROUP BY 1
),
sess AS (
    SELECT playback_mode AS feature, COUNT(*) AS sessions
    FROM ANALYTICS.CURATED.FACT_SESSION WHERE NOT is_zap GROUP BY 1
)
SELECT c.feature, c.cost_usd,
       IFF(c.feature = 'LIVE_BUFFER', NULL,
           c.cost_usd / SUM(IFF(c.feature = 'LIVE_BUFFER', 0, c.cost_usd)) OVER ()) AS incremental_cost_share,
       s.sessions,
       s.sessions / SUM(s.sessions) OVER () AS session_share
FROM cost c
LEFT JOIN sess s ON s.feature = IFF(c.feature = 'LIVE_BUFFER', 'LIVE', c.feature);
