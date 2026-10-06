"""Pandas implementation of the sessionization logic.

This mirrors ``src/spark_sessionize.py`` (PySpark, production) and ``sql/02_fact_session.sql``
(Snowflake) rule for rule so the notebooks can run on a laptop-sized sample:

1. Drop exact duplicate events (telemetry retries resend the same payload).
2. Drop events outside the extraction window (device clock resets produce 10-year-old timestamps).
3. Order each device's events by time. A session starts at every TUNE event and ends at the
   device's next event (TUNE or STANDBY), capped at INACTIVITY_TIMEOUT_MIN when the next event is
   missing or far away (boxes that lose power never send STANDBY).
4. Sessions shorter than ZAP_THRESHOLD_SEC are flagged as channel surfing (zaps); they stay in the
   fact table but are excluded from viewing metrics by default.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

WINDOW_START = pd.Timestamp("2025-03-03")
WINDOW_END = pd.Timestamp("2025-04-01")          # exclusive; one spill-over day for late sittings
INACTIVITY_TIMEOUT_MIN = 240
ZAP_THRESHOLD_SEC = 30
EVENT_COLS = ["device_id", "event_time", "event_type", "channel_id", "playback_mode", "content_age_min"]


def load_events(path: str | Path) -> pd.DataFrame:
    ev = pd.read_csv(path, dtype={"channel_id": "string", "playback_mode": "string",
                                  "content_age_min": "Int64"}, parse_dates=["event_time"])
    return ev


def clean_events(ev: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    stats = {"raw_events": len(ev)}
    ev = ev.drop_duplicates(subset=EVENT_COLS)
    stats["duplicates_removed"] = stats["raw_events"] - len(ev)
    in_window = (ev.event_time >= WINDOW_START) & (ev.event_time < WINDOW_END)
    stats["out_of_window_removed"] = int((~in_window).sum())
    ev = ev[in_window]
    stats["clean_events"] = len(ev)
    return ev, stats


def sessionize(ev: pd.DataFrame) -> pd.DataFrame:
    """Turn clean events into one row per viewing session."""
    ev = ev.sort_values(["device_id", "event_time", "event_type"], kind="stable").reset_index(drop=True)
    same_dev = ev.device_id.eq(ev.device_id.shift(-1))
    next_time = ev.event_time.shift(-1).where(same_dev)
    next_type = ev.event_type.shift(-1).where(same_dev)

    tunes = ev.event_type.eq("TUNE")
    s = ev.loc[tunes, ["device_id", "event_time", "channel_id", "playback_mode", "content_age_min"]].rename(
        columns={"event_time": "session_start"})
    nt, ntype = next_time[tunes], next_type[tunes]
    cap = s.session_start + pd.Timedelta(minutes=INACTIVITY_TIMEOUT_MIN)
    timed_out = nt.isna() | (nt > cap)
    s["session_end"] = nt.where(~timed_out, cap)
    s["end_reason"] = np.where(timed_out, "TIMEOUT", np.where(ntype.eq("STANDBY"), "STANDBY", "NEXT_TUNE"))
    s["duration_sec"] = (s.session_end - s.session_start).dt.total_seconds().astype(int)
    s["is_zap"] = s.duration_sec < ZAP_THRESHOLD_SEC
    s["session_date"] = s.session_start.dt.normalize()
    s["start_hour"] = s.session_start.dt.hour
    s["channel_id"] = s.channel_id.fillna("")
    s.insert(0, "session_id", np.arange(1, len(s) + 1))
    return s.reset_index(drop=True)


def build_sessions(raw_path: str | Path, cache: str | Path | None = None) -> tuple[pd.DataFrame, dict]:
    """Load + clean + sessionize, optionally caching the result as a compressed CSV."""
    if cache is not None and Path(cache).exists():
        s = pd.read_csv(cache, parse_dates=["session_start", "session_end", "session_date"],
                        dtype={"channel_id": "string", "content_age_min": "Int64"})
        s["channel_id"] = s.channel_id.fillna("")
        return s, {}
    ev, stats = clean_events(load_events(raw_path))
    s = sessionize(ev)
    stats["sessions"] = len(s)
    if cache is not None:
        s.to_csv(cache, index=False, compression={"method": "gzip", "mtime": 0})
    return s, stats
