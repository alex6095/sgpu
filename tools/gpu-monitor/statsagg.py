"""statsagg: aggregation over raw JSONL samples and daily rollups.

Invariants
----------
- Streaming reads: every file is read line-by-line in constant memory;
  gzip.open(path, "rt") handles ".gz" transparently.
- Crash-safe input: the writer flushes one JSON object per line, so the last
  line of a live file may be partial. Malformed/truncated lines are skipped
  silently.
- Sums, not averages: per-owner accumulators store SUMS (weighted sums plus
  weights) so that two days' rollups merge by element-wise addition and equal
  a single build over the concatenated raw. Averages are computed only at
  render time.
- Bounded credit: time credit per sample is dt = min(gap_to_previous,
  2 * interval_hint) so a monitor outage never over-credits a single sample.
  interval_hint is the median gap within the day (fallback 15s); the first
  sample of a day gets dt = interval_hint.
- KST is UTC+9 fixed (no DST).
"""

import gzip
import json
import os
import time
from datetime import datetime, timedelta, timezone


DEFAULT_INTERVAL_HINT = 15
KST_OFFSET_HOURS = 9
NONE_OWNER = "?"
SPARK = "▁▂▃▄▅▆▇█"  # ▁▂▃▄▅▆▇█

# Calendar ("grass") cell glyphs, 5 levels (index 0 = empty).
GRASS_UNICODE = " .░▒▓█"   # empty + . + 4 shades (level 5 uses █)
GRASS_ASCII = " .-=*#"     # ascii fallback, same shape
# 256-color backgrounds for the color grass: empty (dark gray) + green scale.
GRASS_BG = ["238", "22", "28", "34", "40", "46"]
# 256-color foregrounds for the heatmap spark gradient (5 intensity buckets).
SPARK_FG = ["22", "28", "34", "40", "46"]

# --- ANSI helpers -----------------------------------------------------------

_ANSI = {
    "bold": "1",
    "dim": "2",
    "yellow": "33",
    "cyan": "36",
}


def _c(code, s, color):
    if not color or code not in _ANSI:
        return s
    return "\x1b[%sm%s\x1b[0m" % (_ANSI[code], s)


def _bg256(code, s):
    """Wrap s in a 256-color background, reset after."""
    return "\x1b[48;5;%sm%s\x1b[0m" % (code, s)


def _fg256(code, s):
    """Wrap s in a 256-color foreground, reset after."""
    return "\x1b[38;5;%sm%s\x1b[0m" % (code, s)


# --- time parsing -----------------------------------------------------------

def _parse_ts(ts):
    """Parse an ISO UTC 'Z' timestamp to epoch seconds, or None."""
    if not ts:
        return None
    try:
        dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except (ValueError, TypeError):
        pass
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


def _kst_hour(epoch):
    """KST hour (0..23) for an epoch second, UTC+9 fixed."""
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc) + timedelta(hours=KST_OFFSET_HOURS)
    return dt.hour


# --- streaming raw readers --------------------------------------------------

def _open_text(path):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8")
    return open(path, "r", encoding="utf-8")


def _iter_records(path):
    """Yield parsed sample dicts from a raw file, skipping malformed lines."""
    with _open_text(path) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except (ValueError, TypeError):
                continue
            if isinstance(rec, dict):
                yield rec


def _raw_path_for(data_dir, date_str):
    raw = os.path.join(data_dir, "samples-%s.jsonl" % date_str)
    gz = raw + ".gz"
    if os.path.isfile(raw):
        return raw
    if os.path.isfile(gz):
        return gz
    return None


# --- empty accumulator shapes ----------------------------------------------

def _empty_owner():
    return {
        "gpu_seconds": 0.0,
        "sm_wsum": 0.0,
        "sm_weight": 0.0,
        "util_wsum": 0.0,
        "util_weight": 0.0,
        "mem_wsum": 0.0,
        "mem_weight": 0.0,
        "mem_peak_mib": 0,
        "mem_total_mib": 0,
        "alloc_gpu_seconds": 0.0,
        "idle_gpu_seconds": 0.0,
        "hour_hist_kst": [0.0] * 24,
    }


def _empty_pod():
    return {
        "owner": None,
        "first_ts": None,
        "last_ts": None,
        "max_gpus": 0,
        "peak_mem_mib": 0,
        "gpu_seconds": 0.0,
        "req": 0,
    }


def _empty_rollup(date_str=""):
    return {
        "v": 1,
        "date": date_str,
        "samples": 0,
        "first_ts": None,
        "last_ts": None,
        "coverage_seconds": 0.0,
        "pods_coverage_seconds": 0.0,
        "interval_hint": DEFAULT_INTERVAL_HINT,
        "node": None,
        "gpu_names": [],
        "owners": {},
        "pods": {},
    }


# --- core accounting --------------------------------------------------------

def _infer_interval_hint(records):
    """Median gap (seconds) between consecutive samples; fallback 15."""
    ts = []
    for rec in records:
        e = _parse_ts(rec.get("ts"))
        if e is not None:
            ts.append(e)
    ts.sort()
    gaps = []
    for i in range(1, len(ts)):
        g = ts[i] - ts[i - 1]
        if g > 0:
            gaps.append(g)
    if not gaps:
        return DEFAULT_INTERVAL_HINT
    gaps.sort()
    n = len(gaps)
    if n % 2:
        return gaps[n // 2]
    return (gaps[n // 2 - 1] + gaps[n // 2]) / 2.0


def _owner_of(proc):
    o = proc.get("owner")
    if o is None:
        return NONE_OWNER
    return o


def build_rollup(day_path):
    """Aggregate one day's raw file into a rollup dict.

    day_path may be a .jsonl or .jsonl.gz path. Returns the rollup dict; on an
    absent/empty file returns a zeroed rollup with whatever date is in the name.
    """
    return _build_rollup(day_path)


def _build_rollup(day_path, idle_options=None):
    date_str = _date_from_path(day_path)
    roll = _empty_rollup(date_str)

    if not os.path.isfile(day_path):
        if idle_options is not None:
            return roll, []
        return roll

    # Two passes: first infer the interval hint (median gap), then accumulate.
    # Both stream; only timestamps are materialized by _infer_interval_hint.
    interval_hint = _infer_interval_hint(_iter_records(day_path))
    roll["interval_hint"] = interval_hint
    dt_cap = 2.0 * interval_hint

    gpu_names = set()
    owners = roll["owners"]
    pods = roll["pods"]
    idle_seen = {}
    idle_proc_pods = set()
    if idle_options is not None:
        owner_filter, now_fn = idle_options
        idle_now = now_fn().timestamp()
        idle_window_start = idle_now - 30 * 60
    else:
        owner_filter = None
        idle_now = None
        idle_window_start = None

    prev_ts = None
    for rec in _iter_records(day_path):
        ts = _parse_ts(rec.get("ts"))
        if ts is None:
            continue
        if idle_options is not None and ts >= idle_window_start:
            _scan_idle_record(rec, ts, idle_seen, idle_proc_pods)

        if prev_ts is None:
            dt = float(interval_hint)
        else:
            gap = ts - prev_ts
            if gap < 0:
                gap = 0.0
            dt = min(gap, dt_cap)
        prev_ts = ts

        roll["samples"] += 1
        roll["coverage_seconds"] += dt
        if roll["first_ts"] is None or ts < _parse_ts(roll["first_ts"]):
            roll["first_ts"] = rec.get("ts")
        if roll["last_ts"] is None or ts > _parse_ts(roll["last_ts"]):
            roll["last_ts"] = rec.get("ts")
        if roll["node"] is None and rec.get("node"):
            roll["node"] = rec.get("node")

        # Index GPU device stats for this sample.
        gpu_util = {}      # gpu index -> util (may be None)
        gpu_mem_total = {}
        for g in rec.get("gpus", []) or []:
            gi = g.get("i")
            if gi is None:
                continue
            gpu_util[gi] = g.get("util")
            if g.get("mem_total") is not None:
                gpu_mem_total[gi] = g.get("mem_total")
            if g.get("name"):
                gpu_names.add(g.get("name"))

        # Group this sample's procs by owner and by pod.
        procs = rec.get("procs", []) or []

        # owner -> set of active gpu indexes
        owner_gpus = {}
        # (owner, gpu) -> summed proc mem on that gpu
        owner_gpu_mem = {}
        # gpu -> set of owners sharing that gpu (for device util split)
        gpu_owners = {}
        # pod -> {gpus:set, mem:int}
        pod_sample = {}
        # owner -> list of sm values (with dt weight applied per proc)
        owner_sm = {}

        for p in procs:
            owner = _owner_of(p)
            gi = p.get("gpu")
            owner_gpus.setdefault(owner, set())
            if gi is not None:
                owner_gpus[owner].add(gi)
                gpu_owners.setdefault(gi, set()).add(owner)
                mem = p.get("mem")
                if mem is not None:
                    key = (owner, gi)
                    owner_gpu_mem[key] = owner_gpu_mem.get(key, 0) + mem

            sm = p.get("sm")
            if sm is not None:
                owner_sm.setdefault(owner, []).append(sm)

            pod = p.get("pod")
            if pod is not None:
                ps = pod_sample.setdefault(pod, {"gpus": set(), "mem": 0,
                                                 "owner": owner})
                if gi is not None:
                    ps["gpus"].add(gi)
                if p.get("mem") is not None:
                    ps["mem"] += p.get("mem")

        # Accumulate per owner.
        for owner, gset in owner_gpus.items():
            acc = owners.setdefault(owner, _empty_owner())
            n_active = len(gset)
            acc["gpu_seconds"] += n_active * dt

            # SM-based util: each proc's sm weighted by dt.
            for sm in owner_sm.get(owner, []):
                acc["sm_wsum"] += sm * dt
                acc["sm_weight"] += dt

            # Device-based util fallback: per active gpu, split by sharers.
            for gi in gset:
                util = gpu_util.get(gi)
                if util is not None:
                    sharers = len(gpu_owners.get(gi, set())) or 1
                    acc["util_wsum"] += (util * dt) / sharers
                    acc["util_weight"] += dt

            # Memory: per active gpu, this owner's summed proc mem.
            sample_mem_peak = 0
            for gi in gset:
                m = owner_gpu_mem.get((owner, gi), 0)
                if m > sample_mem_peak:
                    sample_mem_peak = m
                acc["mem_wsum"] += m * dt
                acc["mem_weight"] += dt
                mt = gpu_mem_total.get(gi)
                if mt is not None and mt > acc["mem_total_mib"]:
                    acc["mem_total_mib"] = mt
            if sample_mem_peak > acc["mem_peak_mib"]:
                acc["mem_peak_mib"] = sample_mem_peak

            # KST hour histogram weighted by active gpu-seconds.
            hh = _kst_hour(ts)
            acc["hour_hist_kst"][hh] += n_active * dt

        # Per-pod accumulation (proc side).
        for pod, ps in pod_sample.items():
            pd = pods.setdefault(pod, _empty_pod())
            if pd["owner"] is None:
                pd["owner"] = ps["owner"]
            if pd["first_ts"] is None or ts < _parse_ts(pd["first_ts"]):
                pd["first_ts"] = rec.get("ts")
            if pd["last_ts"] is None or ts > _parse_ts(pd["last_ts"]):
                pd["last_ts"] = rec.get("ts")
            ng = len(ps["gpus"])
            if ng > pd["max_gpus"]:
                pd["max_gpus"] = ng
            if ps["mem"] > pd["peak_mem_mib"]:
                pd["peak_mem_mib"] = ps["mem"]
            pd["gpu_seconds"] += ng * dt

        # Allocation accounting: only when the record carries pod API rows.
        pod_rows = rec.get("pods")
        if pod_rows is not None:
            roll["pods_coverage_seconds"] += dt
            # Count running allocation per owner and record req per pod.
            alloc_running = {}   # owner -> summed running req
            running_count = {}   # owner -> count of running pods (gpu units)
            for r in pod_rows:
                if r.get("phase") != "Running":
                    continue
                owner = r.get("owner")
                if owner is None:
                    owner = NONE_OWNER
                req = r.get("req") or 0
                alloc_running[owner] = alloc_running.get(owner, 0) + req
                running_count[owner] = running_count.get(owner, 0) + req

                podname = r.get("pod")
                if podname is not None:
                    pd = pods.setdefault(podname, _empty_pod())
                    if pd["owner"] is None:
                        pd["owner"] = owner
                    if req > pd["req"]:
                        pd["req"] = req

            for owner, req_sum in alloc_running.items():
                acc = owners.setdefault(owner, _empty_owner())
                acc["alloc_gpu_seconds"] += req_sum * dt
                active = len(owner_gpus.get(owner, set()))
                idle = running_count.get(owner, 0) - active
                if idle < 0:
                    idle = 0
                acc["idle_gpu_seconds"] += idle * dt

    roll["gpu_names"] = sorted(gpu_names)
    if idle_options is not None:
        return roll, _idle_rows(idle_seen, idle_proc_pods, idle_now,
                                owner_filter)
    return roll


def build_rollup_with_current_idle(day_path, owner_filter=None, now_fn=None):
    """Build today's rollup and current-idle rows in one accumulation pass."""
    if now_fn is None:
        now_fn = lambda: datetime.now(timezone.utc)
    return _build_rollup(day_path, idle_options=(owner_filter, now_fn))


def _date_from_path(path):
    base = os.path.basename(path)
    # samples-YYYYMMDD.jsonl[.gz] or rollup-YYYYMMDD.json
    for prefix in ("samples-", "rollup-"):
        if base.startswith(prefix):
            rest = base[len(prefix):]
            date_str = rest.split(".", 1)[0]
            if len(date_str) == 8 and date_str.isdigit():
                return date_str
    return ""


def write_rollup(day_path):
    """Build a rollup for day_path and write rollup-YYYYMMDD.json beside it."""
    roll = build_rollup(day_path)
    date_str = roll.get("date") or _date_from_path(day_path)
    out_dir = os.path.dirname(os.path.abspath(day_path))
    out_path = os.path.join(out_dir, "rollup-%s.json" % date_str)
    tmp = out_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(roll, fh, ensure_ascii=False)
        fh.flush()
    os.replace(tmp, out_path)
    return roll


# --- merging ----------------------------------------------------------------

def _min_ts(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return a if _parse_ts(a) <= _parse_ts(b) else b


def _max_ts(a, b):
    if a is None:
        return b
    if b is None:
        return a
    return a if _parse_ts(a) >= _parse_ts(b) else b


def _merge_owner(dst, src):
    for key in ("gpu_seconds", "sm_wsum", "sm_weight", "util_wsum",
                "util_weight", "mem_wsum", "mem_weight", "alloc_gpu_seconds",
                "idle_gpu_seconds"):
        dst[key] = dst.get(key, 0.0) + src.get(key, 0.0)
    for key in ("mem_peak_mib", "mem_total_mib"):
        dst[key] = max(dst.get(key, 0), src.get(key, 0))
    dh = dst.setdefault("hour_hist_kst", [0.0] * 24)
    sh = src.get("hour_hist_kst", [0.0] * 24)
    for i in range(24):
        dh[i] = dh[i] + (sh[i] if i < len(sh) else 0.0)


def _merge_pod(dst, src):
    if dst.get("owner") is None:
        dst["owner"] = src.get("owner")
    dst["first_ts"] = _min_ts(dst.get("first_ts"), src.get("first_ts"))
    dst["last_ts"] = _max_ts(dst.get("last_ts"), src.get("last_ts"))
    dst["max_gpus"] = max(dst.get("max_gpus", 0), src.get("max_gpus", 0))
    dst["peak_mem_mib"] = max(dst.get("peak_mem_mib", 0),
                              src.get("peak_mem_mib", 0))
    dst["gpu_seconds"] = dst.get("gpu_seconds", 0.0) + src.get("gpu_seconds", 0.0)
    dst["req"] = max(dst.get("req", 0), src.get("req", 0))


def merge_rollups(rollups):
    """Element-wise merge of a list of rollup dicts (sums add, peaks max).

    Returns a single merged rollup. Exposed for tests.
    """
    out = _empty_rollup("")
    out["date"] = None
    dates = []
    gpu_names = set()

    for roll in rollups:
        if not roll:
            continue
        out["samples"] += roll.get("samples", 0)
        out["coverage_seconds"] += roll.get("coverage_seconds", 0.0)
        out["pods_coverage_seconds"] += roll.get("pods_coverage_seconds", 0.0)
        out["first_ts"] = _min_ts(out["first_ts"], roll.get("first_ts"))
        out["last_ts"] = _max_ts(out["last_ts"], roll.get("last_ts"))
        if out["node"] is None and roll.get("node"):
            out["node"] = roll.get("node")
        for nm in roll.get("gpu_names", []) or []:
            gpu_names.add(nm)
        if roll.get("date"):
            dates.append(roll.get("date"))

        for owner, acc in (roll.get("owners") or {}).items():
            dst = out["owners"].setdefault(owner, _empty_owner())
            _merge_owner(dst, acc)

        for pod, pacc in (roll.get("pods") or {}).items():
            dst = out["pods"].setdefault(pod, _empty_pod())
            _merge_pod(dst, pacc)

    out["gpu_names"] = sorted(gpu_names)
    # interval_hint of a merge is informational only; keep the last non-default.
    hints = [r.get("interval_hint") for r in rollups
             if r and r.get("interval_hint")]
    if hints:
        out["interval_hint"] = hints[-1]
    out["dates"] = sorted(dates)
    return out


# --- current idle detection -------------------------------------------------

def _scan_idle_record(rec, ts, seen, procs_pods):
    """Update idle-now state from one already-parsed raw sample."""
    for p in rec.get("procs", []) or []:
        pod = p.get("pod")
        if pod is not None:
            procs_pods.add(pod)

    pod_rows = rec.get("pods")
    if pod_rows is None:
        return
    for r in pod_rows:
        if r.get("phase") != "Running":
            continue
        req = r.get("req") or 0
        if req <= 0:
            continue
        podname = r.get("pod")
        if podname is None:
            continue
        entry = seen.setdefault(podname, {
            "owner": r.get("owner") if r.get("owner") is not None else NONE_OWNER,
            "req": req,
            "first_seen": ts,
        })
        if req > entry["req"]:
            entry["req"] = req
        if ts < entry["first_seen"]:
            entry["first_seen"] = ts


def _idle_rows(seen, procs_pods, now_epoch, owner_filter):
    out = []
    for pod, entry in seen.items():
        if pod in procs_pods:
            continue
        if owner_filter is not None and entry["owner"] != owner_filter:
            continue
        idle_minutes = int(round((now_epoch - entry["first_seen"]) / 60.0))
        out.append({
            "pod": pod,
            "owner": entry["owner"],
            "req": entry["req"],
            "idle_minutes": idle_minutes,
        })
    out.sort(key=lambda x: (-x["req"], x["pod"]))
    return out


def _current_idle(data_dir, today_str, owner_filter, now_fn):
    """Running GPU pods with zero attributed procs across the last ~30 min.

    Returns [{pod, owner, req, idle_minutes}]. A pod counts as idle-now only if
    it appears Running in the tail window and never had any attributed proc in
    that window.
    """
    path = _raw_path_for(data_dir, today_str)
    if path is None:
        return []

    now = now_fn().timestamp()
    window_start = now - 30 * 60

    # pod -> {"owner", "req", "running": bool, "had_proc": bool,
    #         "first_seen": ts}
    seen = {}
    procs_pods = set()  # pods that had at least one attributed proc in window

    for rec in _iter_records(path):
        ts = _parse_ts(rec.get("ts"))
        if ts is None or ts < window_start:
            continue
        _scan_idle_record(rec, ts, seen, procs_pods)

    return _idle_rows(seen, procs_pods, now, owner_filter)


# --- query ------------------------------------------------------------------

def _filter_owner(roll, owner):
    """Return a copy of roll restricted to a single owner (and its pods)."""
    if owner is None:
        return roll
    out = dict(roll)
    owners = roll.get("owners") or {}
    out["owners"] = {owner: owners[owner]} if owner in owners else {}
    pods = roll.get("pods") or {}
    out["pods"] = {k: v for k, v in pods.items() if v.get("owner") == owner}
    return out


def query(data_dir, days=7, owner=None, now_fn=None):
    """Aggregate the last `days` UTC dates ending today.

    For each date: use rollup-YYYYMMDD.json if present, else build from raw if
    the raw/gz file exists (today has no rollup yet). Merge them. Also compute
    current_idle from the last ~30 min of today's samples.
    """
    if now_fn is None:
        now_fn = lambda: datetime.now(timezone.utc)

    now = now_fn()
    today = now.date()
    today_str = today.strftime("%Y%m%d")

    rollups = []
    dates_covered = []
    daily = []
    current_idle = []
    current_idle_done = False
    for back in range(days - 1, -1, -1):
        d = today - timedelta(days=back)
        date_str = d.strftime("%Y%m%d")
        roll = None

        rollup_path = os.path.join(data_dir, "rollup-%s.json" % date_str)
        if date_str != today_str and os.path.isfile(rollup_path):
            try:
                with open(rollup_path, "r", encoding="utf-8") as fh:
                    roll = json.load(fh)
            except Exception:
                roll = None

        if roll is None:
            raw = _raw_path_for(data_dir, date_str)
            if raw is not None:
                try:
                    if date_str == today_str:
                        roll, current_idle = build_rollup_with_current_idle(
                            raw, owner_filter=owner, now_fn=now_fn)
                        current_idle_done = True
                    else:
                        roll = build_rollup(raw)
                except Exception:
                    roll = None

        if roll is not None and roll.get("samples", 0) >= 0:
            has_data = roll.get("samples", 0) > 0 or roll.get("owners")
            if has_data:
                filtered = _filter_owner(roll, owner)
                rollups.append(filtered)
                dates_covered.append(date_str)
                # Reuse the per-day (owner-filtered) rollup we just loaded;
                # no second file read.
                day_owners = {}
                for own, acc in (filtered.get("owners") or {}).items():
                    day_owners[own] = acc.get("gpu_seconds", 0.0)
                daily.append({
                    "date": date_str,
                    "owners": day_owners,
                    "coverage_seconds": filtered.get("coverage_seconds", 0.0),
                })

    merged = merge_rollups(rollups)

    if not current_idle_done:
        current_idle = _current_idle(data_dir, today_str, owner, now_fn)

    # Structured awards so JSON consumers (the TUI stats view) don't have to
    # duplicate the threshold logic. Text format is "Title: owner — detail".
    awards = []
    for key, own, text in _compute_awards(
            merged.get("owners") or {},
            merged.get("pods_coverage_seconds", 0.0),
            merged.get("coverage_seconds", 0.0)):
        title, _, rest = text.partition(": ")
        _owner_part, sep, detail = rest.partition(" — ")
        awards.append({
            "key": key,
            "icon": _award_prefix(key, True),
            "ascii": _award_prefix(key, False),
            "title": title,
            "owner": own,
            "detail": detail if sep else rest,
        })

    return {
        "window_days": days,
        "dates_covered": dates_covered,
        "merged": merged,
        "current_idle": current_idle,
        "daily": daily,
        "awards": awards,
        "generated_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# --- rendering --------------------------------------------------------------

def _avg(wsum, weight):
    if not weight:
        return None
    return wsum / weight


def _eff_util(acc):
    """Effective util percent: prefer SM average, else device average."""
    sm = _avg(acc.get("sm_wsum", 0.0), acc.get("sm_weight", 0.0))
    if sm is not None:
        return sm
    return _avg(acc.get("util_wsum", 0.0), acc.get("util_weight", 0.0))


def _owner_width(owners):
    """Leaderboard/label owner column width: max name len clamped to [8, 20]."""
    if not owners:
        return 8
    w = max(len(str(o)) for o in owners)
    if w < 8:
        w = 8
    if w > 20:
        w = 20
    return w


def _clip(s, width):
    s = str(s)
    if len(s) <= width:
        return s
    return s[:width]


def _bucket_level(value, mx, nlevels):
    """Map value in [0, mx] to a level in 0..nlevels.

    0 stays level 0 (empty); positive values spread across 1..nlevels by
    quartile-style fractions of the row max.
    """
    if value <= 0 or mx <= 0:
        return 0
    frac = value / mx
    if frac > 1.0:
        frac = 1.0
    lvl = int(frac * nlevels + 0.9999)  # ceil-ish so tiny > 0 -> level 1
    if lvl < 1:
        lvl = 1
    if lvl > nlevels:
        lvl = nlevels
    return lvl


# --- award computation ------------------------------------------------------

# Each award: (key, emoji, ascii_tag). Priority = list order (used for the
# max-3-awards-per-owner rule and to keep output stable).
_AWARDS = [
    ("best", "🏆", "[best]"),
    ("power", "⚡", "[power]"),
    ("sharp", "🎯", "[sharp]"),
    ("mem", "🧠", "[mem]"),
    ("night", "🦉", "[night]"),
    ("headroom", "💤", "[headroom]"),
    ("seat", "🪑", "[seat]"),
]


def _compute_awards(owners, pods_cov, coverage_seconds):
    """Return an ordered list of (award_key, owner, text) that meet thresholds.

    owners excludes the "?" owner. Never raises on empty/one-owner data.
    Applies the max-3-awards-per-owner rule (drop lowest-priority extras).
    """
    items = [(o, a) for o, a in owners.items() if o != NONE_OWNER]

    def gpu_h(acc):
        return acc.get("gpu_seconds", 0.0) / 3600.0

    def sm_avg(acc):
        return _avg(acc.get("sm_wsum", 0.0), acc.get("sm_weight", 0.0))

    def util_pct(acc):
        # avg util prefers SM, else device util
        return _eff_util(acc)

    raw = {}  # award_key -> (owner, text)

    # 🏆 Best researcher: highest effective GPU-h, require util>=40% and >=1 GPU-h
    best = None
    for o, acc in items:
        gh = gpu_h(acc)
        u = util_pct(acc)
        if u is None or gh < 1.0 or u < 40.0:
            continue
        eff_h = gh * (u / 100.0)
        if best is None or eff_h > best[1]:
            best = (o, eff_h, u, gh)
    if best is not None:
        o, eff_h, u, gh = best
        raw["best"] = (o, "Best researcher: %s — %.1f effective GPU-h "
                          "(%.0f%% avg over %.1f GPU-h)" % (o, eff_h, u, gh))

    # ⚡ Power user: most GPU-h (>= 1 GPU-h)
    power = None
    for o, acc in items:
        gh = gpu_h(acc)
        if gh < 1.0:
            continue
        if power is None or gh > power[1]:
            power = (o, gh)
    if power is not None:
        o, gh = power
        raw["power"] = (o, "Power user: %s — %.1f GPU-h" % (o, gh))

    # 🎯 Sharpshooter: highest avg SM% (>= 2 GPU-h)
    sharp = None
    for o, acc in items:
        gh = gpu_h(acc)
        sm = sm_avg(acc)
        if sm is None or gh < 2.0:
            continue
        if sharp is None or sm > sharp[1]:
            sharp = (o, sm, gh)
    if sharp is not None:
        o, sm, gh = sharp
        raw["sharp"] = (o, "Sharpshooter: %s — %.0f%% avg SM over %.1f GPU-h"
                           % (o, sm, gh))

    # 🧠 Memory heavyweight: highest peak mem (>= 32 GiB peak)
    mem = None
    for o, acc in items:
        peak_gib = acc.get("mem_peak_mib", 0) / 1024.0
        if peak_gib < 32.0:
            continue
        if mem is None or peak_gib > mem[1]:
            mem = (o, peak_gib)
    if mem is not None:
        o, peak_gib = mem
        raw["mem"] = (o, "Memory heavyweight: %s — %.1f GiB peak"
                         % (o, peak_gib))

    # 🦉 Night owl: largest share of own activity in KST 0-5 (>= 1 GPU-h in win)
    night = None
    for o, acc in items:
        hh = acc.get("hour_hist_kst", [0.0] * 24)
        total = sum(hh) if hh else 0.0
        night_s = sum(hh[h] for h in range(6) if h < len(hh)) if hh else 0.0
        if night_s / 3600.0 < 1.0 or total <= 0:
            continue
        share = night_s / total
        if night is None or share > night[1]:
            night = (o, share)
    if night is not None:
        o, share = night
        raw["night"] = (o, "Night owl: %s — %.0f%% of activity in KST 0-5h"
                           % (o, share * 100.0))

    # 💤 Most headroom: lowest avg util among owners with >= 4 GPU-h, util < 40%
    headroom = None
    for o, acc in items:
        gh = gpu_h(acc)
        u = util_pct(acc)
        if u is None or gh < 4.0 or u >= 40.0:
            continue
        if headroom is None or u < headroom[1]:
            headroom = (o, u, gh)
    if headroom is not None:
        o, u, gh = headroom
        raw["headroom"] = (o, "Most headroom: %s — %.0f%% avg util over "
                              "%.0f GPU-h (free speedup waiting)" % (o, u, gh))

    # 🪑 Seat warmer: most idle-allocation hours; only when pod coverage strong
    if pods_cov > 0.5 * coverage_seconds:
        seat = None
        for o, acc in items:
            idle_h = acc.get("idle_gpu_seconds", 0.0) / 3600.0
            if idle_h < 2.0:
                continue
            if seat is None or idle_h > seat[1]:
                seat = (o, idle_h)
        if seat is not None:
            o, idle_h = seat
            raw["seat"] = (o, "Seat warmer: %s — %.1f idle GPU-h allocated"
                              % (o, idle_h))

    # Assemble in priority order.
    ordered = []
    for key, _emoji, _tag in _AWARDS:
        if key in raw:
            o, text = raw[key]
            ordered.append((key, o, text))

    # Max 3 awards per owner: keep highest-priority (earliest in list) three.
    counts = {}
    kept = []
    for key, o, text in ordered:
        n = counts.get(o, 0)
        if n >= 3:
            continue
        counts[o] = n + 1
        kept.append((key, o, text))
    return kept


def _award_prefix(key, unicode_ok):
    for k, emoji, tag in _AWARDS:
        if k == key:
            return emoji if unicode_ok else tag
    return ""


def render_stats_text(result, color=False, width=100, unicode_ok=True):
    """Render a query() result to a plain (or minimally ANSI) text report."""
    merged = result.get("merged") or _empty_rollup("")
    owners = merged.get("owners") or {}
    window_days = result.get("window_days", 0)
    dates = result.get("dates_covered") or []
    daily = result.get("daily") or []
    coverage_h = merged.get("coverage_seconds", 0.0) / 3600.0
    coverage_seconds = merged.get("coverage_seconds", 0.0)
    pods_cov = merged.get("pods_coverage_seconds", 0.0)

    lines = []

    # --- a. title + subtitle ---
    lines.append(_c("cyan", _c("bold", "SGPU usage report — last %d days"
                               % window_days, color), color))
    day_word = "day" if len(dates) == 1 else "days"
    subtitle = "data: %d %s, coverage %.1fh" % (len(dates), day_word, coverage_h)
    lines.append(_c("dim", subtitle, color))
    lines.append("")

    if not owners and merged.get("samples", 0) == 0:
        lines.append("no samples recorded yet")
        return "\n".join(lines) + "\n"

    ranked = sorted(owners.items(),
                    key=lambda kv: kv[1].get("gpu_seconds", 0.0),
                    reverse=True)
    ow = _owner_width(owners)

    # --- b. awards ---
    awards = _compute_awards(owners, pods_cov, coverage_seconds)
    if awards:
        lines.append(_c("bold", "Awards", color))
        for key, _o, text in awards:
            prefix = _award_prefix(key, unicode_ok)
            lines.append("%s %s" % (prefix, text))
        lines.append("")

    # --- c. leaderboard ---
    lines.append(_c("bold", "Leaderboard", color))
    header = ("%-*s %6s %6s %8s %10s %9s %8s %8s %6s"
              % (ow, "OWNER", "GPU-H", "EFF-H", "AVG-SM%", "AVG-UTIL%",
                 "PEAK-MEM", "ALLOC-H", "IDLE-H", "IDLE%"))
    lines.append(_c("bold", header, color))

    for owner, acc in ranked:
        gpu_h = acc.get("gpu_seconds", 0.0) / 3600.0
        sm = _avg(acc.get("sm_wsum", 0.0), acc.get("sm_weight", 0.0))
        util = _avg(acc.get("util_wsum", 0.0), acc.get("util_weight", 0.0))
        eff = _eff_util(acc)
        peak_gib = acc.get("mem_peak_mib", 0) / 1024.0

        eff_s = "%.1f" % (gpu_h * (eff / 100.0)) if eff is not None else ""
        sm_s = "%.0f" % sm if sm is not None else ""
        util_s = "%.0f" % util if util is not None else ""
        peak_s = "%.1f" % peak_gib

        if pods_cov > 0:
            alloc_h = acc.get("alloc_gpu_seconds", 0.0) / 3600.0
            idle_h = acc.get("idle_gpu_seconds", 0.0) / 3600.0
            alloc_gs = acc.get("alloc_gpu_seconds", 0.0)
            idle_pct = (acc.get("idle_gpu_seconds", 0.0) / alloc_gs * 100.0
                        if alloc_gs > 0 else 0.0)
            alloc_s = "%.1f" % alloc_h
            idle_s = "%.1f" % idle_h
            idlep_s = "%.0f" % idle_pct
        else:
            alloc_s = ""
            idle_s = ""
            idlep_s = ""

        row = ("%-*s %6s %6s %8s %10s %9s %8s %8s %6s"
               % (ow, _clip(owner, ow), "%.1f" % gpu_h, eff_s, sm_s, util_s,
                  peak_s, alloc_s, idle_s, idlep_s))
        lines.append(row)

    if pods_cov == 0:
        lines.append(_c("dim",
                        "(allocation stats unavailable: no pod API coverage)",
                        color))
    lines.append("")

    # --- d. daily activity calendar (the "grass") ---
    if len(dates) >= 2:
        _render_grass(lines, ranked, daily, dates, ow, color, unicode_ok, width)

    # --- e. KST hour heatmap ---
    # Same visual language as the grass calendar: a contiguous strip of
    # 2-char background-colored cells per hour (empty hours get the dark
    # base cell), so the strip reads as one bar instead of sparse dots.
    lines.append(_c("bold", "KST hour activity (gpu-seconds share)", color))
    ticks = [" "] * 48
    for h in range(0, 24, 3):
        for j, ch in enumerate(str(h)):
            if 2 * h + j < 48:
                ticks[2 * h + j] = ch
    lines.append(_c("dim", "%-*s %s" % (ow, "KST", "".join(ticks)), color))
    glyphs = GRASS_UNICODE if unicode_ok else GRASS_ASCII

    def heat_row(label, hist):
        mx = max(hist) if hist else 0.0
        parts = []
        for v in hist:
            lvl = _bucket_level(v, mx, 5)
            if color:
                parts.append(_bg256(GRASS_BG[lvl], "  "))
            else:
                parts.append(glyphs[lvl] * 2)
        return "%-*s %s" % (ow, _clip(label, ow), "".join(parts))

    top8 = ranked[:8]
    total_hist = [0.0] * 24
    for owner, acc in ranked:
        hh = acc.get("hour_hist_kst", [0.0] * 24)
        for i in range(24):
            total_hist[i] += hh[i] if i < len(hh) else 0.0

    for owner, acc in top8:
        lines.append(heat_row(owner, acc.get("hour_hist_kst", [0.0] * 24)))
    lines.append(heat_row("TOTAL", total_hist))
    lines.append("")

    # --- f. warnings ---
    warnings = []
    for owner, acc in ranked:
        gpu_h = acc.get("gpu_seconds", 0.0) / 3600.0
        if gpu_h < 4:
            continue
        eff = _eff_util(acc)
        if eff is not None and eff < 30:
            warnings.append("LOW-UTIL %s avg %.0f%% over %.0f GPU-h"
                            % (owner, eff, gpu_h))

    for entry in result.get("current_idle") or []:
        warnings.append("IDLE-NOW pod %s (%d GPU) idle %dm"
                        % (entry.get("pod"), entry.get("req", 0),
                           entry.get("idle_minutes", 0)))

    if warnings:
        lines.append(_c("bold", "Warnings", color))
        for w in warnings:
            lines.append(_c("yellow", "  " + w, color))
        lines.append("")

    # Drop a trailing blank line for tidiness.
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines) + "\n"


def _date_range(first, last):
    """Inclusive list of YYYYMMDD strings from first..last (calendar order)."""
    try:
        a = datetime.strptime(first, "%Y%m%d")
        b = datetime.strptime(last, "%Y%m%d")
    except (ValueError, TypeError):
        return [first, last] if first != last else [first]
    out = []
    cur = a
    while cur <= b:
        out.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return out


def _render_grass(lines, ranked, daily, dates, ow, color, unicode_ok, width):
    """Append the daily-activity calendar block to `lines`.

    One row per owner (top 8 by gpu_seconds) plus a TOTAL row, one 2-char cell
    per date in the window (oldest->newest, INCLUDING dates with no data,
    rendered as the empty cell). Cell level is bucketed by fraction of the
    ROW's own max. When the window has more dates than fit in `width`, keep the
    MOST RECENT dates and prefix the row with '…'.
    """
    lines.append(_c("bold", "Daily activity", color))

    # date -> {owner -> gpu_seconds} lookup from the per-day series.
    by_date = {}
    for d in daily:
        by_date[d.get("date")] = d.get("owners") or {}

    # Full contiguous span from earliest to latest covered date: gap days with
    # no data still get a (empty) cell, like a GitHub contribution graph.
    all_dates = _date_range(dates[0], dates[-1]) if dates else []

    grass = GRASS_UNICODE if unicode_ok else GRASS_ASCII
    nlevels = len(grass) - 1  # 5 non-empty levels

    # Layout: label(ow) + 1 space + optional '…'(1) + 2*ncells <= width.
    # Cells are 2 chars wide in both modes.
    budget = width - (ow + 1)
    if budget < 2:
        budget = 2
    truncated = len(all_dates) * 2 > budget
    if truncated:
        # reserve 1 col for the '…' marker; keep most-recent dates that fit
        keep = (budget - 1) // 2
        if keep < 1:
            keep = 1
        shown_dates = all_dates[-keep:]
    else:
        shown_dates = list(all_dates)
    mark = "…" if truncated else ""

    def cell_str(level):
        if color:
            return _bg256(GRASS_BG[level], "  ")  # two-space colored bg cell
        ch = grass[level]
        return "  " if level == 0 else ch + ch    # doubled glyph, 2 chars wide

    # --- date tick header: MM/DD under every 7th cell starting at the first ---
    # Each cell occupies 2 columns; cell i starts at column 2*i within the
    # cells region. The cells region begins after label(ow)+space+mark.
    prefix_cols = ow + 1 + len(mark)
    tick = [" "] * prefix_cols
    for i, ds in enumerate(shown_dates):
        if i % 7 != 0 or len(ds) != 8:
            continue
        label = "%s/%s" % (ds[4:6], ds[6:8])  # MM/DD, 5 chars
        col = prefix_cols + 2 * i
        while len(tick) < col + len(label):
            tick.append(" ")
        for j, ch in enumerate(label):
            tick[col + j] = ch
    lines.append(_c("dim", "".join(tick).rstrip(), color))

    def grass_row(disp, is_total):
        row_vals = []
        for ds in shown_dates:
            owns = by_date.get(ds, {})
            if is_total:
                row_vals.append(sum(owns.values()))
            else:
                row_vals.append(float(owns.get(disp, 0.0)))
        mx = max(row_vals) if row_vals else 0.0
        cells = "".join(cell_str(_bucket_level(v, mx, nlevels)) for v in row_vals)
        return "%-*s %s%s" % (ow, _clip(disp, ow), mark, cells)

    for owner, _acc in ranked[:8]:
        lines.append(grass_row(owner, False))
    lines.append(grass_row("TOTAL", True))

    # --- legend ---
    if color:
        swatches = "".join(_bg256(GRASS_BG[lv], "  ")
                           for lv in range(len(grass)))
        legend = "less " + swatches + " more"
    else:
        legend = "less " + grass + " more"
    lines.append(_c("dim", legend, color))
    lines.append("")


# --- file listing / raw streaming (for server endpoints) --------------------

def list_files(data_dir):
    """[{name, size, mtime_iso}] for samples-* and rollup-*, sorted by name."""
    out = []
    try:
        names = os.listdir(data_dir)
    except Exception:
        return out
    for name in sorted(names):
        if not (name.startswith("samples-") or name.startswith("rollup-")):
            continue
        full = os.path.join(data_dir, name)
        if not os.path.isfile(full):
            continue
        st = os.stat(full)
        mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
        out.append({
            "name": name,
            "size": st.st_size,
            "mtime_iso": mtime.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
    return out


def iter_raw_lines(data_dir, date_str):
    """Yield decoded raw lines for samples-YYYYMMDD.jsonl[.gz].

    Raises FileNotFoundError if neither the raw nor the gz file exists.
    """
    path = _raw_path_for(data_dir, date_str)
    if path is None:
        raise FileNotFoundError(
            "no samples file for %s in %s" % (date_str, data_dir))

    def _lines():
        with _open_text(path) as fh:
            for line in fh:
                yield line.rstrip("\n")

    return _lines()
