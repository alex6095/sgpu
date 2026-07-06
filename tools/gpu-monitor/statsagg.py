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
    date_str = _date_from_path(day_path)
    roll = _empty_rollup(date_str)

    if not os.path.isfile(day_path):
        return roll

    # Two passes: first infer the interval hint (median gap), then accumulate.
    # Both stream; we materialize timestamps only, not full records.
    records = list(_iter_records(day_path))
    interval_hint = _infer_interval_hint(records)
    roll["interval_hint"] = interval_hint
    dt_cap = 2.0 * interval_hint

    gpu_names = set()
    owners = roll["owners"]
    pods = roll["pods"]

    prev_ts = None
    for rec in records:
        ts = _parse_ts(rec.get("ts"))
        if ts is None:
            continue

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
    return roll


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

        for p in rec.get("procs", []) or []:
            pod = p.get("pod")
            if pod is not None:
                procs_pods.add(pod)

        pod_rows = rec.get("pods")
        if pod_rows is None:
            continue
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

    out = []
    for pod, entry in seen.items():
        if pod in procs_pods:
            continue
        if owner_filter is not None and entry["owner"] != owner_filter:
            continue
        idle_minutes = int(round((now - entry["first_seen"]) / 60.0))
        out.append({
            "pod": pod,
            "owner": entry["owner"],
            "req": entry["req"],
            "idle_minutes": idle_minutes,
        })
    out.sort(key=lambda x: (-x["req"], x["pod"]))
    return out


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
                    roll = build_rollup(raw)
                except Exception:
                    roll = None

        if roll is not None and roll.get("samples", 0) >= 0:
            has_data = roll.get("samples", 0) > 0 or roll.get("owners")
            if has_data:
                rollups.append(_filter_owner(roll, owner))
                dates_covered.append(date_str)

    merged = merge_rollups(rollups)

    current_idle = _current_idle(data_dir, today_str, owner, now_fn)

    return {
        "window_days": days,
        "dates_covered": dates_covered,
        "merged": merged,
        "current_idle": current_idle,
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


def render_stats_text(result, color=False, width=100):
    """Render a query() result to a plain (or minimally ANSI) text report."""
    merged = result.get("merged") or _empty_rollup("")
    owners = merged.get("owners") or {}
    window_days = result.get("window_days", 0)
    dates = result.get("dates_covered") or []
    coverage_h = merged.get("coverage_seconds", 0.0) / 3600.0
    pods_cov = merged.get("pods_coverage_seconds", 0.0)

    lines = []

    title = ("SGPU usage report — last %d days (%d days of data, coverage %.1fh)"
             % (window_days, len(dates), coverage_h))
    lines.append(_c("cyan", title, color))
    lines.append("")

    if not owners and merged.get("samples", 0) == 0:
        lines.append("no samples recorded yet")
        return "\n".join(lines) + "\n"

    # --- leaderboard ---
    header = ("%-12s %7s %8s %10s %9s %8s %8s %6s"
              % ("OWNER", "GPU-H", "AVG-SM%", "AVG-UTIL%", "PEAK-MEM",
                 "ALLOC-H", "IDLE-H", "IDLE%"))
    lines.append(_c("bold", header, color))

    ranked = sorted(owners.items(),
                    key=lambda kv: kv[1].get("gpu_seconds", 0.0),
                    reverse=True)

    for owner, acc in ranked:
        gpu_h = acc.get("gpu_seconds", 0.0) / 3600.0
        sm = _avg(acc.get("sm_wsum", 0.0), acc.get("sm_weight", 0.0))
        util = _avg(acc.get("util_wsum", 0.0), acc.get("util_weight", 0.0))
        peak_gib = acc.get("mem_peak_mib", 0) / 1024.0

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

        row = ("%-12s %7s %8s %10s %9s %8s %8s %6s"
               % (str(owner)[:12], "%.1f" % gpu_h, sm_s, util_s, peak_s,
                  alloc_s, idle_s, idlep_s))
        lines.append(row)

    if pods_cov == 0:
        lines.append(_c("dim",
                        "(allocation stats unavailable: no pod API coverage)",
                        color))
    lines.append("")

    # --- warnings ---
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

    # --- KST hour heatmap ---
    lines.append(_c("bold", "KST hour activity (gpu-seconds share)", color))
    header_cells = " ".join("%2d" % h for h in range(24))
    lines.append("KST " + header_cells)

    def spark_row(label, hist):
        mx = max(hist) if hist else 0.0
        cells = []
        for v in hist:
            if v <= 0 or mx <= 0:
                cells.append(" ")
            else:
                idx = int((v / mx) * (len(SPARK) - 1) + 0.5)
                if idx < 0:
                    idx = 0
                if idx >= len(SPARK):
                    idx = len(SPARK) - 1
                cells.append(SPARK[idx])
        # two-char columns to align with numeric header
        body = " ".join(" " + ch for ch in cells)
        return "%-3s %s" % (str(label)[:3], body)

    top8 = ranked[:8]
    total_hist = [0.0] * 24
    for owner, acc in ranked:
        hh = acc.get("hour_hist_kst", [0.0] * 24)
        for i in range(24):
            total_hist[i] += hh[i] if i < len(hh) else 0.0

    for owner, acc in top8:
        lines.append(spark_row(owner, acc.get("hour_hist_kst", [0.0] * 24)))
    lines.append(spark_row("TOT", total_hist))

    return "\n".join(lines) + "\n"


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
    raw = os.path.join(data_dir, "samples-%s.jsonl" % date_str)
    gz = raw + ".gz"
    if os.path.isfile(raw):
        path = raw
    elif os.path.isfile(gz):
        path = gz
    else:
        raise FileNotFoundError(
            "no samples file for %s in %s" % (date_str, data_dir))
    with _open_text(path) as fh:
        for line in fh:
            yield line.rstrip("\n")
