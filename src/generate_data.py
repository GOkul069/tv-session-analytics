"""Deterministic synthetic data generator for the TV session analytics project.

Simulates four weeks (2025-03-03 to 2025-03-30) of raw set-top box (STB) telemetry for a
fictional IPTV operator: a sample of 10,000 devices tuning across 126
channels, plus a channel maintenance log and daily storage usage by playback feature.

Raw tables written to data/raw/:
  devices.csv                 one row per sampled set-top box
  channels.csv                channel lineup (126 channels) with feature flags
  stb_events.csv.gz           raw TUNE / STANDBY telemetry (gzip CSV, ~1.6M rows)
  maintenance_events.csv      channel maintenance windows (planned and unplanned)
  storage_usage_daily.csv     GB stored per day x channel x feature
  storage_rate_card.csv       storage price per GB-month by tier

Planted structure that the notebooks are expected to discover:
  * prime-time viewing peak (18:00-22:00)
  * planned maintenance that ops pushes at the end of the business day, so most windows land in
    prime time; unplanned incidents scale with load and also cluster in prime time
  * StartOver and catch-up use few sessions but keep large rolling archives for every channel
  * data quality issues in the raw feed: duplicate events, clock-skewed timestamps, and sittings
    with no STANDBY event (handled by the sessionization timeout rule)

Full scale: the production feed behind this design held 1.5B+ session records. The default
sample keeps every CSV below 20 MB. Use --devices to change the sample size, e.g.
    python src/generate_data.py --devices 40000   # ~6.5M events, ~4x the files
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

SEED = 7
START = pd.Timestamp("2025-03-03")
N_DAYS = 28
RAW = Path(__file__).resolve().parents[1] / "data" / "raw"

GENRES = {  # genre: (n_channels, popularity weight, startover share, catchup days choices)
    "News": (14, 1.3, 0.9, [0, 3]),
    "Sports": (16, 1.5, 0.95, [3, 7]),
    "Movies": (18, 1.0, 0.8, [7]),
    "Entertainment": (24, 1.4, 0.95, [7]),
    "Kids": (12, 1.0, 0.9, [7]),
    "Documentary": (12, 0.7, 0.85, [7]),
    "Lifestyle": (12, 0.6, 0.8, [3, 7]),
    "Music": (8, 0.4, 0.5, [0]),
    "Regional": (10, 0.5, 0.7, [0, 3, 7]),
}
MODES = ["LIVE", "STARTOVER", "CATCHUP", "CLOUD_DVR", "VOD"]
MODE_P = [0.755, 0.065, 0.10, 0.05, 0.03]

# share of sitting start times by hour of day (local time)
HOUR_W = np.array([1.2, 0.6, 0.3, 0.2, 0.2, 0.3, 1.2, 2.5, 2.4, 1.8, 1.6, 1.7,
                   2.2, 2.1, 1.9, 2.1, 2.8, 4.0, 7.0, 9.5, 10.0, 8.5, 5.0, 2.6])


def build_channels(rng):
    rows, i = [], 1
    for g, (n, w, so, cu) in GENRES.items():
        for k in range(n):
            rows.append({
                "channel_id": f"CH{i:03d}",
                "channel_name": f"{g} {k + 1:02d}",
                "genre": g,
                "is_hd": bool(rng.random() < 0.8),
                "startover_enabled": bool(rng.random() < so),
                "catchup_window_days": int(rng.choice(cu)),
                "popularity": float(w * rng.pareto(1.6) + 0.05 * w),
            })
            i += 1
    ch = pd.DataFrame(rows)
    ch["popularity"] = (ch.popularity / ch.popularity.sum()).round(6)
    return ch


def build_devices(rng, n):
    return pd.DataFrame({
        "device_id": [f"STB{i:06d}" for i in range(1, n + 1)],
        "region": rng.choice(["North", "South", "East", "West", "Central"], n, p=[.22, .24, .2, .18, .16]),
        "box_model": rng.choice(["LX-200", "LX-300", "LX-400 4K"], n, p=[.35, .45, .2]),
        "package": rng.choice(["Basic", "Plus", "Premium"], n, p=[.45, .38, .17]),
        "activation_date": (pd.Timestamp("2019-01-01")
                            + pd.to_timedelta(rng.integers(0, 2200, n), unit="D")).strftime("%Y-%m-%d"),
    })


def build_events(rng, devices, channels):
    n_dev = len(devices)
    activity = rng.beta(4, 1.5, n_dev)                    # probability device is used on a day
    sit_rate = rng.gamma(4, 0.42, n_dev)                  # sittings per active day
    # device-day grid
    dev = np.repeat(np.arange(n_dev), N_DAYS)
    day = np.tile(np.arange(N_DAYS), n_dev)
    weekend = (START + pd.to_timedelta(day, unit="D")).dayofweek >= 5
    active = rng.random(dev.size) < activity[dev] * np.where(weekend, 1.05, 1.0)
    n_sit = rng.poisson(sit_rate[dev] * active * np.where(weekend, 1.15, 1.0))
    s_dev = np.repeat(dev, n_sit)
    s_day = np.repeat(day, n_sit)
    hour = rng.choice(24, s_dev.size, p=HOUR_W / HOUR_W.sum())
    s_start = s_day * 86400 + hour * 3600 + rng.integers(0, 3600, s_dev.size)
    n_tunes = rng.geometric(0.3, s_dev.size)            # tunes per sitting, mean ~3.3

    t_sit = np.repeat(np.arange(s_dev.size), n_tunes)
    is_zap = rng.random(t_sit.size) < 0.22
    dur = np.where(is_zap, rng.integers(2, 25, t_sit.size), rng.exponential(1500, t_sit.size) + 60)
    # longer viewing in prime time
    t_hour = hour[t_sit]
    dur = np.where((t_hour >= 18) & (t_hour < 23) & ~is_zap, dur * 1.35, dur).astype(int)
    offset = np.cumsum(dur) - dur
    first = np.r_[0, np.cumsum(n_tunes)[:-1]]
    offset = offset - np.repeat(offset[first], n_tunes)
    t_time = s_start[t_sit] + offset
    t_dev = s_dev[t_sit]

    # channel choice: popularity with device-level genre affinity (approximated by noise)
    pop = channels.popularity.values
    t_ch = rng.choice(len(channels), t_sit.size, p=pop / pop.sum())
    mode = rng.choice(len(MODES), t_sit.size, p=MODE_P)
    # features unavailable on a channel fall back to LIVE
    so_ok = channels.startover_enabled.values[t_ch]
    cu_ok = channels.catchup_window_days.values[t_ch] > 0
    mode = np.where((mode == 1) & ~so_ok, 0, mode)
    mode = np.where((mode == 2) & ~cu_ok, 0, mode)
    mode_s = np.array(MODES)[mode]
    ch_s = channels.channel_id.values[t_ch].astype(object)
    ch_s[mode_s == "VOD"] = ""

    # minutes between original broadcast and playback (time-shifted modes only)
    window_min = channels.catchup_window_days.values[t_ch] * 1440
    age = np.full(t_sit.size, np.nan)
    age = np.where(mode == 1, rng.uniform(1, 120, t_sit.size), age)
    cu_age = np.minimum(rng.exponential(1500, t_sit.size) + 30, np.maximum(window_min, 60) - 1)
    age = np.where(mode == 2, cu_age, age)
    age = np.where(mode == 3, rng.exponential(4000, t_sit.size) + 30, age)
    age_s = pd.array(np.round(age), dtype="Int64")

    tune = pd.DataFrame({"dev": t_dev, "ts": t_time, "event_type": "TUNE",
                         "channel_id": ch_s, "playback_mode": mode_s, "content_age_min": age_s})
    last = np.cumsum(n_tunes) - 1
    sit_end = t_time[last] + dur[last]
    has_standby = rng.random(s_dev.size) > 0.03          # 3% sittings never send STANDBY
    stby = pd.DataFrame({"dev": s_dev[has_standby], "ts": sit_end[has_standby], "event_type": "STANDBY",
                         "channel_id": "", "playback_mode": "",
                         "content_age_min": pd.array([pd.NA] * int(has_standby.sum()), dtype="Int64")})
    ev = pd.concat([tune, stby], ignore_index=True)

    # data quality issues
    dup = ev.sample(frac=0.004, random_state=1)
    skew = ev.sample(frac=0.0015, random_state=2).copy()
    skew["ts"] = skew.ts - 86400 * 365 * 10              # device clock reset
    ev = pd.concat([ev, dup, skew], ignore_index=True)
    ev = ev.sample(frac=1.0, random_state=3)              # arrival order != event order

    ev["device_id"] = devices.device_id.values[ev.dev.values]
    ev["event_time"] = (START + pd.to_timedelta(ev.ts, unit="s")).dt.strftime("%Y-%m-%d %H:%M:%S")
    return ev[["device_id", "event_time", "event_type", "channel_id", "playback_mode", "content_age_min"]]


def build_maintenance(rng, channels):
    rows, mid = [], 1
    load = HOUR_W / HOUR_W.sum()
    planned_hour = np.full(24, 0.3)
    planned_hour[9:17] = 1.6
    planned_hour[18:22] = 9.0        # ops push queue released at end of business day
    planned_hour /= planned_hour.sum()
    unplanned_hour = load ** 1.6
    unplanned_hour /= unplanned_hour.sum()
    types_planned = ["Encoder firmware update", "Stream config push", "EPG metadata refresh"]
    types_unplanned = ["Encoder restart (incident)", "Origin failover (incident)"]
    for ch in channels.itertuples():
        n = rng.poisson(3.5 + 40 * ch.popularity)
        for _ in range(n):
            planned = rng.random() < 0.6
            hour = rng.choice(24, p=planned_hour if planned else unplanned_hour)
            day = rng.integers(0, N_DAYS)
            start = START + pd.Timedelta(days=int(day), hours=int(hour), minutes=int(rng.integers(0, 60)))
            dur = int(np.clip(rng.lognormal(np.log(22 if planned else 35), 0.45), 5, 120))
            rows.append({
                "maintenance_id": f"MX{mid:05d}",
                "channel_id": ch.channel_id,
                "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                "duration_min": dur,
                "maintenance_type": rng.choice(types_planned if planned else types_unplanned),
                "is_planned": planned,
                "deferrable": bool(planned and rng.random() < 0.75),
            })
            mid += 1
    return pd.DataFrame(rows).sort_values("start_time").reset_index(drop=True)


def build_storage(rng, channels):
    gb_per_hour = np.where(channels.is_hd, 11.0, 5.5)     # full ABR ladder, all renditions
    rows = []
    vod_lib = 118_000.0
    for d in range(N_DAYS):
        date = (START + pd.Timedelta(days=d)).strftime("%Y-%m-%d")
        for i, ch in enumerate(channels.itertuples()):
            jitter = rng.normal(1, 0.02)
            rows.append((date, ch.channel_id, "LIVE_BUFFER", gb_per_hour[i] * 2 * jitter, "hot"))
            if ch.startover_enabled:
                rows.append((date, ch.channel_id, "STARTOVER", gb_per_hour[i] * 24 * jitter, "hot"))
            if ch.catchup_window_days:
                rows.append((date, ch.channel_id, "CATCHUP",
                             gb_per_hour[i] * 24 * ch.catchup_window_days * jitter, "standard"))
            dvr = (1_100 + 90_000 * ch.popularity) * gb_per_hour[i] / 11 * (1 + 0.004 * d) * jitter
            rows.append((date, ch.channel_id, "CLOUD_DVR", dvr, "standard"))
        rows.append((date, "", "VOD", vod_lib * (1 + 0.002 * d) * rng.normal(1, 0.005), "standard"))
    st = pd.DataFrame(rows, columns=["usage_date", "channel_id", "feature", "stored_gb", "storage_tier"])
    st["stored_gb"] = st.stored_gb.round(1)
    rate = pd.DataFrame({"storage_tier": ["hot", "standard"],
                         "usd_per_gb_month": [0.045, 0.021],
                         "description": ["low-latency SSD origin cache", "object storage, standard class"]})
    return st, rate


def main(n_devices: int = 10_000) -> None:
    rng = np.random.default_rng(SEED)
    RAW.mkdir(parents=True, exist_ok=True)
    channels = build_channels(rng)
    devices = build_devices(rng, n_devices)
    events = build_events(rng, devices, channels)
    maint = build_maintenance(rng, channels)
    storage, rate = build_storage(rng, channels)

    channels.drop(columns=["popularity"]).to_csv(RAW / "channels.csv", index=False)
    devices.to_csv(RAW / "devices.csv", index=False)
    events.to_csv(RAW / "stb_events.csv.gz", index=False, compression={"method": "gzip", "mtime": 0})
    maint.to_csv(RAW / "maintenance_events.csv", index=False)
    storage.to_csv(RAW / "storage_usage_daily.csv", index=False)
    rate.to_csv(RAW / "storage_rate_card.csv", index=False)
    for f in sorted(RAW.glob("*.csv*")):
        n = len(pd.read_csv(f, usecols=[0]))
        print(f"{f.name:26s} {n:>10,d} rows  {f.stat().st_size / 1e6:6.1f} MB")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--devices", type=int, default=10_000, help="number of sampled set-top boxes")
    main(ap.parse_args().devices)
