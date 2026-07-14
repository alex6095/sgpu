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
  2 * local_cadence) so a monitor outage never over-credits a single sample.
  local_cadence follows confirmed, sustained within-day cadence changes while
  ``interval_hint`` remains the historical daily median (fallback 15s).
- KST is UTC+9 fixed (no DST).
- Rollup compatibility: a v1 rollup is used for owner statistics, but when
  its preserved raw file is available it is lazily rebuilt to v2 before any
  cluster telemetry is shown.  If raw history has already expired, pulse
  fields stay absent rather than pretending missing telemetry was 0%.
"""

import gzip
import json
import os
import tempfile
import threading
import time
from datetime import datetime, timedelta, timezone


DEFAULT_INTERVAL_HINT = 15
# A cadence change is only accepted after three adjacent, similar gaps.  This
# is deliberately small enough to recognize a sampler changing 15 -> 60s
# promptly, while rejecting one missed sample or a short collector stall.
CADENCE_TRANSITION_CONFIRMATIONS = 3
CADENCE_MATCH_TOLERANCE = 0.25
CADENCE_SLOW_RATIO = 2.0
CADENCE_FAST_RATIO = 0.5
KST_OFFSET_HOURS = 9
NONE_OWNER = "?"
SPARK = "▁▂▃▄▅▆▇█"  # ▁▂▃▄▅▆▇█

# Rollup v2 adds cluster-level device telemetry.  It is intentionally derived
# from the GPU fields that v1 samples already recorded, rather than changing
# the sampler or keeping a second high-volume time series.  The ten buckets
# preserve enough shape for a useful utilization profile without making daily
# rollups large.
ROLLUP_VERSION = 2
UTIL_BUCKET_SIZE = 10
UTIL_BUCKETS = 10
BUSY_UTIL_THRESHOLD = 50

# A read-only stats volume cannot accept the lazy v1 -> v2 rewrite.  Keep the
# freshly rebuilt value in-process, keyed by source file metadata, so requests
# do not repeatedly decompress the same historical raw file.  The server's
# short query cache handles the common path too; this is the safe fallback.
_LEGACY_REBUILD_CACHE = {}
_LEGACY_REBUILD_CACHE_MAX = 512
_LEGACY_REBUILD_CACHE_LOCK = threading.Lock()
# POSIX permits simultaneous replacements of the same destination, whereas
# Windows can transiently reject the second replace.  Temp-file creation and
# JSON writes remain concurrent; only the final rename is serialized.
_ROLLUP_REPLACE_LOCK = threading.Lock()

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


def _empty_cluster():
    """Accumulator for device telemetry, independent of process attribution.

    ``device_seconds`` is the total time for which a GPU reported a numeric
    utilization value.  Memory has its own weight because older records may
    lack either ``mem`` or ``mem_total``.  The hourly arrays are KST, matching
    the owner activity heatmap.

    Busy/idle windows are deliberately conservative: a busy window means at
    least one observed GPU was at or above BUSY_UTIL_THRESHOLD.  They describe
    *observed* monitoring periods, not job starts, which makes them safe across
    monitor outages and daily rollup boundaries.
    """
    return {
        "device_seconds": 0.0,
        "util_wsum": 0.0,
        "util_weight": 0.0,
        "mem_wsum": 0.0,
        "mem_weight": 0.0,
        "hour_util_wsum": [0.0] * 24,
        "hour_util_weight": [0.0] * 24,
        "hour_mem_wsum": [0.0] * 24,
        "hour_mem_weight": [0.0] * 24,
        "util_hist": [0.0] * UTIL_BUCKETS,
        "busy_windows": 0,
        "busy_seconds": 0.0,
        "idle_windows": 0,
        "idle_seconds": 0.0,
        "longest_busy_seconds": 0.0,
        "longest_idle_seconds": 0.0,
        # Boundary metadata makes Flow a mergeable daily summary.  The
        # counters above are additive, but a busy run crossing midnight must
        # be collapsed once when adjacent rollups are merged.  These fields
        # retain just the two edge runs, not a per-sample timeline.
        "flow_first_state": None,
        "flow_last_state": None,
        "flow_leading_seconds": 0.0,
        "flow_trailing_seconds": 0.0,
        "flow_first_ts": None,
        "flow_last_ts": None,
        "flow_all_one_run": True,
        "flow_complete": True,
        # "device" means one node's any-GPU windows.  LAB merges cannot
        # reconstruct a global time union from compact rollups, but can report
        # an exact sum of per-node windows when every input is complete.
        "flow_scope": "device",
        # Count of rollup days that actually contained numeric device util.
        # It is diagnostic only; query results carry the exact calendar dates.
        "telemetry_days": 0,
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
        "v": ROLLUP_VERSION,
        "date": date_str,
        "samples": 0,
        "first_ts": None,
        "last_ts": None,
        "coverage_seconds": 0.0,
        "pods_coverage_seconds": 0.0,
        "interval_hint": DEFAULT_INTERVAL_HINT,
        "node": None,
        "gpu_names": [],
        "cluster": _empty_cluster(),
        "owners": {},
        "pods": {},
    }


# --- core accounting --------------------------------------------------------

def _median(values):
    """Return the exact median of a non-empty numeric sequence."""
    values = sorted(values)
    n = len(values)
    if n % 2:
        return values[n // 2]
    return (values[n // 2 - 1] + values[n // 2]) / 2.0


def _interval_hint_from_timestamps(timestamps):
    """Historical daily median gap, ignoring non-monotonic corrupt lines."""
    gaps = []
    prev_ts = None
    for ts in timestamps:
        if prev_ts is None:
            prev_ts = ts
            continue
        if ts > prev_ts:
            gaps.append(ts - prev_ts)
            prev_ts = ts
    if not gaps:
        return DEFAULT_INTERVAL_HINT
    return _median(gaps)


def _infer_interval_hint(records):
    """Median gap (seconds) between samples; retained for v1 callers."""
    timestamps = []
    for rec in records:
        epoch = _parse_ts(rec.get("ts"))
        if epoch is not None:
            timestamps.append(epoch)
    return _interval_hint_from_timestamps(timestamps)


def _similar_cadence(left, right):
    """Whether two positive gaps plausibly describe the same cadence."""
    if left <= 0 or right <= 0:
        return False
    ratio = left / right
    return (1.0 - CADENCE_MATCH_TOLERANCE
            <= ratio <= 1.0 + CADENCE_MATCH_TOLERANCE)


def _transition_direction(gap, cadence):
    """Return 1/-1 for a plausible slower/faster cadence, else None."""
    if gap <= 0 or cadence <= 0:
        return None
    ratio = gap / cadence
    if CADENCE_SLOW_RATIO < ratio:
        return 1
    if ratio < CADENCE_FAST_RATIO:
        return -1
    return None


def _cadence_profile(timestamps, fallback):
    """Return ``(initial_cadence, [(sample_index, cadence), ...])``.

    The profile is calculated from the first pass' file-order timestamps.  A
    segment starts at the *first* confirmed gap, allowing the second streaming
    pass to credit all three confirming samples at their new cadence.  An
    outlier never becomes a segment: it must be followed by two similar gaps.
    There is intentionally no transition-size ceiling: a deliberate sampler
    update may legally move from 15 seconds to many minutes.  The daily median
    is not changed, preserving rollup compatibility and reporting.
    """
    initial = float(fallback)
    current = None
    segments = []
    candidate = []  # [(sample index, gap)]
    candidate_direction = None
    bootstrap_clean = True
    first_positive_index = None
    prev_ts = None

    for index, ts in enumerate(timestamps):
        if prev_ts is None:
            prev_ts = ts
            continue
        # A duplicate or out-of-order line must not manufacture a cadence
        # change.  Retain the latest monotonic timestamp for the next gap.
        if ts <= prev_ts:
            continue
        gap = ts - prev_ts
        prev_ts = ts
        if first_positive_index is None:
            first_positive_index = index

        if current is None:
            # Bootstrap from any short stable run near the beginning.  Three
            # matching gaps are the evidence; imposing an arbitrary ceiling
            # would break a legitimate large SGPU_SAMPLE_INTERVAL change.
            if not candidate or _similar_cadence(gap, candidate[-1][1]):
                candidate.append((index, gap))
            else:
                candidate = [(index, gap)]
                bootstrap_clean = False

            if len(candidate) < CADENCE_TRANSITION_CONFIRMATIONS:
                continue
            observed = _median([entry[1] for entry in candidate])
            start_index = candidate[0][0]
            if (bootstrap_clean
                    and start_index == first_positive_index):
                # The file began with this cadence, so its first sample also
                # deserves the matching initial credit.
                initial = observed
            else:
                # Before a late stable run, preserve the legacy daily hint.
                # From the run's first gap onwards, use its observed cadence.
                segments.append((start_index, observed))
            current = observed
            candidate = []
            candidate_direction = None
            continue

        direction = _transition_direction(gap, current)
        if direction is None:
            candidate = []
            candidate_direction = None
            continue
        if (candidate and candidate_direction == direction
                and _similar_cadence(gap, candidate[-1][1])):
            candidate.append((index, gap))
        else:
            candidate = [(index, gap)]
            candidate_direction = direction
        if len(candidate) < CADENCE_TRANSITION_CONFIRMATIONS:
            continue

        observed = _median([entry[1] for entry in candidate])
        segments.append((candidate[0][0], observed))
        current = observed
        candidate = []
        candidate_direction = None

    return initial, segments


def _infer_cadence_profile(records):
    """First-pass interval median plus file-order adaptive cadence segments."""
    timestamps = []
    for rec in records:
        epoch = _parse_ts(rec.get("ts"))
        if epoch is not None:
            timestamps.append(epoch)
    interval_hint = _interval_hint_from_timestamps(timestamps)
    initial, segments = _cadence_profile(timestamps, interval_hint)
    return interval_hint, initial, segments


def _owner_of(proc):
    o = proc.get("owner")
    if o is None:
        return NONE_OWNER
    return o


def _number(value):
    """Return a finite float for a numeric-ish value, else None.

    Stats files are deliberately long-lived.  Be strict at the aggregation
    boundary so a hand-edited, old, or partially-null record cannot poison a
    whole rollup with a TypeError or a NaN.
    """
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _percent(value):
    value = _number(value)
    if value is None:
        return None
    return max(0.0, min(100.0, value))


def _accumulate_cluster(cluster, gpus, hour, dt, flow_state):
    """Add one sample's device telemetry and return its flow state.

    ``flow_state`` is ``(busy_bool, run_seconds)`` or ``None``.  It remains a
    local build detail (not persisted), while the resulting windows and their
    longest observed runs are persisted in the rollup.
    """
    utils = []
    for g in gpus or []:
        if not isinstance(g, dict):
            continue
        util = _percent(g.get("util"))
        if util is not None:
            utils.append(util)
            cluster["device_seconds"] += dt
            cluster["util_wsum"] += util * dt
            cluster["util_weight"] += dt
            cluster["hour_util_wsum"][hour] += util * dt
            cluster["hour_util_weight"][hour] += dt
            bucket = min(UTIL_BUCKETS - 1, int(util // UTIL_BUCKET_SIZE))
            cluster["util_hist"][bucket] += dt

        used = _number(g.get("mem"))
        total = _number(g.get("mem_total"))
        if used is not None and total is not None and total > 0:
            mem_pct = max(0.0, min(100.0, 100.0 * used / total))
            cluster["mem_wsum"] += mem_pct * dt
            cluster["mem_weight"] += dt
            cluster["hour_mem_wsum"][hour] += mem_pct * dt
            cluster["hour_mem_weight"][hour] += dt

    # Missing device telemetry is a gap, not an idle period.  Resetting the
    # local state prevents a synthetic multi-hour "streak" through old/null
    # records or a collector failure.
    if not utils:
        return None

    busy = max(utils) >= BUSY_UTIL_THRESHOLD
    if flow_state is not None and flow_state[0] == busy:
        run_seconds = flow_state[1] + dt
    else:
        run_seconds = dt
        cluster["busy_windows" if busy else "idle_windows"] += 1

    if busy:
        cluster["busy_seconds"] += dt
        if run_seconds > cluster["longest_busy_seconds"]:
            cluster["longest_busy_seconds"] = run_seconds
    else:
        cluster["idle_seconds"] += dt
        if run_seconds > cluster["longest_idle_seconds"]:
            cluster["longest_idle_seconds"] = run_seconds
    return busy, run_seconds


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

    # Two passes: the first preserves the historical daily median and derives
    # a compact file-order cadence profile; the second still streams the raw
    # JSONL.  The existing exact median already materializes one day's
    # timestamps, while the profile adds only confirmed segment boundaries.
    interval_hint, initial_cadence, cadence_segments = _infer_cadence_profile(
        _iter_records(day_path))
    roll["interval_hint"] = interval_hint

    gpu_names = set()
    owners = roll["owners"]
    pods = roll["pods"]
    cluster = roll["cluster"]
    cluster_flow = None
    flow_first_state = None
    flow_leading_seconds = 0.0
    flow_leading_open = True
    flow_all_one_run = True
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
    sample_index = -1
    cadence_index = 0
    local_cadence = initial_cadence
    for rec in _iter_records(day_path):
        ts = _parse_ts(rec.get("ts"))
        if ts is None:
            continue
        sample_index += 1
        while (cadence_index < len(cadence_segments)
               and sample_index >= cadence_segments[cadence_index][0]):
            local_cadence = cadence_segments[cadence_index][1]
            cadence_index += 1

        roll["samples"] += 1
        if roll["first_ts"] is None or ts < _parse_ts(roll["first_ts"]):
            roll["first_ts"] = rec.get("ts")
        if roll["last_ts"] is None or ts > _parse_ts(roll["last_ts"]):
            roll["last_ts"] = rec.get("ts")
        if roll["node"] is None and rec.get("node"):
            roll["node"] = rec.get("node")

        # Malformed chronological order has no elapsed interval to credit.
        # In particular, do not move ``prev_ts`` backwards: a late old line
        # must not inflate the following sample's dt or split its Flow run.
        if prev_ts is not None and ts <= prev_ts:
            continue
        if idle_options is not None and ts >= idle_window_start:
            _scan_idle_record(rec, ts, idle_seen, idle_proc_pods)

        if prev_ts is None:
            dt = float(local_cadence)
            flow_gap = False
        else:
            gap = ts - prev_ts
            dt_cap = 2.0 * local_cadence
            # Capping credit prevents an outage from inflating usage.  The
            # same gap is a Flow boundary: Flow is strictly an observed
            # telemetry window, so missing telemetry must break it.
            flow_gap = gap > dt_cap
            dt = min(gap, dt_cap)
        prev_ts = ts

        roll["coverage_seconds"] += dt

        # Index GPU device stats for this sample.  The cluster accumulator is
        # intentionally fed before process attribution so un-attributed work
        # remains visible in the cluster pulse.
        gpu_util = {}      # gpu index -> util (may be None)
        gpu_mem_total = {}
        gpus = rec.get("gpus", []) or []
        for g in gpus:
            if not isinstance(g, dict):
                continue
            gi = g.get("i")
            if gi is None:
                continue
            # Keep the older owner accumulators just as defensive as the new
            # cluster telemetry.  JSON permits NaN in Python's decoder and a
            # historic hand-edited record must not poison a whole report.
            gpu_util[gi] = _percent(g.get("util"))
            mem_total = _number(g.get("mem_total"))
            if mem_total is not None:
                gpu_mem_total[gi] = mem_total
            if g.get("name"):
                gpu_names.add(g.get("name"))

        kst_hour = _kst_hour(ts)
        previous_flow = cluster_flow
        if flow_gap:
            cluster_flow = None
            previous_flow = None
            if flow_first_state is not None:
                flow_leading_open = False
                flow_all_one_run = False
        cluster_flow = _accumulate_cluster(
            cluster, gpus, kst_hour, dt, cluster_flow)
        if cluster_flow is None:
            if previous_flow is not None and flow_first_state is not None:
                flow_leading_open = False
                flow_all_one_run = False
        else:
            flow_state, flow_run_seconds = cluster_flow
            if flow_first_state is None:
                flow_first_state = flow_state
                flow_leading_seconds = flow_run_seconds
                cluster["flow_first_ts"] = rec.get("ts")
            else:
                if (previous_flow is None
                        or previous_flow[0] != flow_state):
                    flow_leading_open = False
                    flow_all_one_run = False
                elif flow_leading_open:
                    flow_leading_seconds = flow_run_seconds
            cluster["flow_last_ts"] = rec.get("ts")
            cluster["flow_last_state"] = flow_state
            cluster["flow_trailing_seconds"] = flow_run_seconds

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
            if not isinstance(p, dict):
                continue
            owner = _owner_of(p)
            gi = p.get("gpu")
            owner_gpus.setdefault(owner, set())
            if gi is not None:
                owner_gpus[owner].add(gi)
                gpu_owners.setdefault(gi, set()).add(owner)
                mem = _number(p.get("mem"))
                if mem is not None:
                    key = (owner, gi)
                    owner_gpu_mem[key] = owner_gpu_mem.get(key, 0) + mem

            sm = _percent(p.get("sm"))
            if sm is not None:
                owner_sm.setdefault(owner, []).append(sm)

            pod = p.get("pod")
            if pod is not None:
                ps = pod_sample.setdefault(pod, {"gpus": set(), "mem": 0,
                                                 "owner": owner})
                if gi is not None:
                    ps["gpus"].add(gi)
                mem = _number(p.get("mem"))
                if mem is not None:
                    ps["mem"] += mem

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
            acc["hour_hist_kst"][kst_hour] += n_active * dt

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
                if not isinstance(r, dict):
                    continue
                if r.get("phase") != "Running":
                    continue
                owner = r.get("owner")
                if owner is None:
                    owner = NONE_OWNER
                req = _number(r.get("req")) or 0
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
    cluster["flow_first_state"] = flow_first_state
    cluster["flow_leading_seconds"] = flow_leading_seconds
    cluster["flow_all_one_run"] = flow_all_one_run
    if cluster["util_weight"] > 0:
        cluster["telemetry_days"] = 1
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


def _rollup_output_path(day_path, roll):
    date_str = roll.get("date") or _date_from_path(day_path)
    out_dir = os.path.dirname(os.path.abspath(day_path))
    return os.path.join(out_dir, "rollup-%s.json" % date_str)


def _write_rollup_file(out_path, roll):
    """Atomically replace one rollup without sharing temp files between writers.

    The stats HTTP server may lazy-upgrade the same v1 rollup from concurrent
    requests.  Each writer owns a unique temp in the destination directory;
    only its own completed file is ever renamed or cleaned up.  ``os.replace``
    makes the final handoff atomic for readers on the same filesystem.
    """
    out_dir = os.path.dirname(os.path.abspath(out_path))
    base = os.path.basename(out_path)
    fd, tmp = tempfile.mkstemp(prefix=".%s." % base, suffix=".tmp", dir=out_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(roll, fh, ensure_ascii=False)
            fh.flush()
            # Make the new file durable before exposing it.  If fsync fails,
            # retain the previous rollup rather than publishing a doubtful one.
            os.fsync(fh.fileno())
        with _ROLLUP_REPLACE_LOCK:
            os.replace(tmp, out_path)
    finally:
        # os.replace removes our temp on success.  On a read-only volume or
        # an interrupted rename, cleanup is best-effort and can only touch the
        # unique path allocated above, never another request's temp file.
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass


def _try_write_rollup(day_path, roll):
    """Best-effort persistence for query-time compatibility upgrades."""
    try:
        _write_rollup_file(_rollup_output_path(day_path, roll), roll)
        return True
    except Exception:
        return False


def write_rollup(day_path):
    """Build a rollup for day_path and atomically write it beside the raw."""
    roll = build_rollup(day_path)
    _write_rollup_file(_rollup_output_path(day_path, roll), roll)
    return roll


# --- merging ----------------------------------------------------------------

def _min_ts(a, b):
    if a is None:
        return b
    if b is None:
        return a
    a_epoch = _parse_ts(a)
    b_epoch = _parse_ts(b)
    if a_epoch is None:
        return b
    if b_epoch is None:
        return a
    return a if a_epoch <= b_epoch else b


def _max_ts(a, b):
    if a is None:
        return b
    if b is None:
        return a
    a_epoch = _parse_ts(a)
    b_epoch = _parse_ts(b)
    if a_epoch is None:
        return b
    if b_epoch is None:
        return a
    return a if a_epoch >= b_epoch else b


def _merge_cluster(dst, src):
    """Merge a v2 cluster accumulator; silently accept v1/no-cluster input."""
    if not isinstance(src, dict):
        return
    for key in ("device_seconds", "util_wsum", "util_weight", "mem_wsum",
                "mem_weight", "busy_windows", "busy_seconds",
                "idle_windows", "idle_seconds", "telemetry_days"):
        value = _number(src.get(key))
        if value is not None:
            dst[key] += value
    for key in ("longest_busy_seconds", "longest_idle_seconds"):
        value = _number(src.get(key))
        if value is not None and value > dst[key]:
            dst[key] = value
    for key in ("hour_util_wsum", "hour_util_weight", "hour_mem_wsum",
                "hour_mem_weight", "util_hist"):
        incoming = src.get(key) or []
        if not isinstance(incoming, (list, tuple)):
            continue
        target = dst[key]
        for i in range(min(len(target), len(incoming))):
            value = _number(incoming[i])
            if value is not None:
                target[i] += value


def _merge_owner(dst, src):
    if not isinstance(src, dict):
        return
    for key in ("gpu_seconds", "sm_wsum", "sm_weight", "util_wsum",
                "util_weight", "mem_wsum", "mem_weight", "alloc_gpu_seconds",
                "idle_gpu_seconds"):
        value = _number(src.get(key))
        if value is not None:
            dst[key] = (_number(dst.get(key)) or 0.0) + value
    for key in ("mem_peak_mib", "mem_total_mib"):
        value = _number(src.get(key))
        if value is not None:
            dst[key] = max(_number(dst.get(key)) or 0.0, value)
    dh = dst.setdefault("hour_hist_kst", [0.0] * 24)
    sh = src.get("hour_hist_kst") or []
    if not isinstance(sh, (list, tuple)):
        sh = []
    for i in range(24):
        incoming = _number(sh[i] if i < len(sh) else None)
        if incoming is not None:
            dh[i] = (_number(dh[i] if i < len(dh) else None) or 0.0) + incoming


def _merge_pod(dst, src):
    if not isinstance(src, dict):
        return
    if dst.get("owner") is None:
        dst["owner"] = src.get("owner")
    dst["first_ts"] = _min_ts(dst.get("first_ts"), src.get("first_ts"))
    dst["last_ts"] = _max_ts(dst.get("last_ts"), src.get("last_ts"))
    for key in ("max_gpus", "peak_mem_mib", "req"):
        value = _number(src.get(key))
        if value is not None:
            dst[key] = max(_number(dst.get(key)) or 0.0, value)
    value = _number(src.get("gpu_seconds"))
    if value is not None:
        dst["gpu_seconds"] = (_number(dst.get("gpu_seconds")) or 0.0) + value


_FLOW_METADATA_KEYS = (
    "flow_first_state", "flow_last_state", "flow_leading_seconds",
    "flow_trailing_seconds", "flow_first_ts", "flow_last_ts",
    "flow_all_one_run", "flow_complete",
)


def _rollup_has_flow_metadata(roll):
    cluster = (roll or {}).get("cluster")
    return isinstance(cluster, dict) and all(key in cluster
                                             for key in _FLOW_METADATA_KEYS)


def _flow_edge_is_contiguous(left, right):
    """Whether two daily flow summaries touch without an observation gap."""
    left_cluster = left.get("cluster") or {}
    right_cluster = right.get("cluster") or {}
    left_end = _parse_ts(left_cluster.get("flow_last_ts"))
    right_start = _parse_ts(right_cluster.get("flow_first_ts"))
    left_raw_end = _parse_ts(left.get("last_ts"))
    right_raw_start = _parse_ts(right.get("first_ts"))
    if None in (left_end, right_start, left_raw_end, right_raw_start):
        return False
    # Any raw sample after/before the final/initial telemetry sample is an
    # explicit missing-telemetry boundary and must break a Flow run.
    if abs(left_end - left_raw_end) > 1e-6 \
            or abs(right_start - right_raw_start) > 1e-6:
        return False
    gap = right_start - left_end
    if gap < 0:
        return False
    left_hint = _number(left.get("interval_hint")) or DEFAULT_INTERVAL_HINT
    right_hint = _number(right.get("interval_hint")) or DEFAULT_INTERVAL_HINT
    # A configuration change from 15s to 60s at midnight is still continuous;
    # using the larger observed cadence avoids inventing a gap at that edge.
    return gap <= 2.0 * max(left_hint, right_hint)


def _copy_flow_boundary(cluster):
    return {key: cluster.get(key) for key in _FLOW_METADATA_KEYS}


def _stitch_flow_boundaries(out, rollups):
    """Collapse equal-state runs that cross adjacent daily rollup boundaries.

    ``merge_rollups`` is also used to add independent nodes for LAB scope. In
    that case a union of time windows is unknowable from compact daily data,
    so callers disable stitching and the Flow line is withheld.  Here the
    ordered input is one node's calendar series, for which edge metadata is
    sufficient and keeps the rollup compact.
    """
    target = out["cluster"]
    known = all(_rollup_has_flow_metadata(roll) for roll in rollups)
    if not known:
        target["flow_complete"] = False
        return

    sequence = None
    previous = None
    for roll in rollups:
        source = roll.get("cluster") or {}
        if source.get("flow_first_state") is None:
            # A no-telemetry day is a truthful gap between observed windows.
            previous = None
            if sequence is not None:
                sequence["flow_all_one_run"] = False
            continue
        if sequence is None:
            sequence = _copy_flow_boundary(source)
            previous = roll
            continue

        touching = previous is not None and _flow_edge_is_contiguous(
            previous, roll)
        same_state = (touching
                      and sequence.get("flow_last_state")
                      == source.get("flow_first_state"))
        if same_state:
            state = source.get("flow_first_state")
            window_key = "busy_windows" if state else "idle_windows"
            target[window_key] = max(0.0, target.get(window_key, 0.0) - 1.0)
            combined = ((_number(sequence.get("flow_trailing_seconds")) or 0.0)
                        + (_number(source.get("flow_leading_seconds")) or 0.0))
            longest_key = ("longest_busy_seconds" if state
                           else "longest_idle_seconds")
            target[longest_key] = max(target.get(longest_key, 0.0), combined)

            prior_all_one = bool(sequence.get("flow_all_one_run"))
            source_all_one = bool(source.get("flow_all_one_run"))
            if prior_all_one:
                sequence["flow_leading_seconds"] = (
                    (_number(sequence.get("flow_leading_seconds")) or 0.0)
                    + (_number(source.get("flow_leading_seconds")) or 0.0))
            if source_all_one:
                sequence["flow_trailing_seconds"] = combined
            else:
                sequence["flow_trailing_seconds"] = source.get(
                    "flow_trailing_seconds")
            sequence["flow_all_one_run"] = prior_all_one and source_all_one
        else:
            # Different state is a real transition; a missing/gapped edge is
            # also a hard break even when both visible states happen to match.
            sequence["flow_all_one_run"] = False
            sequence["flow_trailing_seconds"] = source.get(
                "flow_trailing_seconds")

        sequence["flow_last_state"] = source.get("flow_last_state")
        sequence["flow_last_ts"] = source.get("flow_last_ts")
        previous = roll

    if sequence is not None:
        for key in _FLOW_METADATA_KEYS:
            target[key] = sequence.get(key)
    target["flow_complete"] = True
    target["flow_scope"] = "device"


def merge_rollups(rollups, stitch_flow=True):
    """Element-wise merge of a list of rollup dicts (sums add, peaks max).

    Returns a single merged rollup. Exposed for tests.
    """
    out = _empty_rollup("")
    out["date"] = None
    dates = []
    gpu_names = set()

    usable_rollups = [roll for roll in rollups if isinstance(roll, dict)]
    for roll in usable_rollups:
        samples = _number(roll.get("samples"))
        coverage = _number(roll.get("coverage_seconds"))
        pods_coverage = _number(roll.get("pods_coverage_seconds"))
        if samples is not None:
            # Sample counts are an integer JSON field in every rollup/API
            # version.  Sanitizing through float must not silently change the
            # public shape to ``123.0``.
            out["samples"] += int(max(0.0, samples))
        if coverage is not None:
            out["coverage_seconds"] += max(0.0, coverage)
        if pods_coverage is not None:
            out["pods_coverage_seconds"] += max(0.0, pods_coverage)
        out["first_ts"] = _min_ts(out["first_ts"], roll.get("first_ts"))
        out["last_ts"] = _max_ts(out["last_ts"], roll.get("last_ts"))
        if out["node"] is None and roll.get("node"):
            out["node"] = roll.get("node")
        for nm in roll.get("gpu_names", []) or []:
            gpu_names.add(nm)
        if roll.get("date"):
            dates.append(roll.get("date"))

        _merge_cluster(out["cluster"], roll.get("cluster"))

        owners = roll.get("owners") or {}
        if not isinstance(owners, dict):
            owners = {}
        for owner, acc in owners.items():
            dst = out["owners"].setdefault(owner, _empty_owner())
            _merge_owner(dst, acc)

        pods = roll.get("pods") or {}
        if not isinstance(pods, dict):
            pods = {}
        for pod, pacc in pods.items():
            dst = out["pods"].setdefault(pod, _empty_pod())
            _merge_pod(dst, pacc)

    out["gpu_names"] = sorted(gpu_names)
    # interval_hint of a merge is informational only; keep the last non-default.
    hints = [_number(r.get("interval_hint")) for r in usable_rollups]
    hints = [hint for hint in hints if hint is not None and hint > 0]
    if hints:
        out["interval_hint"] = hints[-1]
    out["dates"] = sorted(dates)
    if stitch_flow:
        _stitch_flow_boundaries(out, usable_rollups)
    else:
        # Per-node Flow windows cannot be unioned into one global time series,
        # but their count sum and longest per-node run are exact.  Expose that
        # explicit scope only if every source has the boundary metadata needed
        # to make its own flow summary trustworthy.
        out["cluster"]["flow_complete"] = (
            all(_rollup_has_flow_metadata(roll)
                for roll in usable_rollups))
        out["cluster"]["flow_scope"] = (
            "node" if len(usable_rollups) > 1 else "device")
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
    """Return a copy of roll restricted to one owner (and its pods).

    Device telemetry is intrinsically cluster-wide, so it is deliberately
    removed from an owner-filtered result rather than being misread as that
    owner's utilization pulse.  Owner efficiency remains available from the
    per-owner accumulators.
    """
    if owner is None:
        return roll
    out = dict(roll)
    owners = roll.get("owners") or {}
    if not isinstance(owners, dict):
        owners = {}
    out["owners"] = {owner: owners[owner]} if owner in owners else {}
    pods = roll.get("pods") or {}
    if not isinstance(pods, dict):
        pods = {}
    out["pods"] = {k: v for k, v in pods.items()
                   if isinstance(v, dict) and v.get("owner") == owner}
    out["cluster"] = _empty_cluster()
    return out


_CLUSTER_REQUIRED_KEYS = (
    "util_weight", "hour_util_wsum", "hour_util_weight", "util_hist",
    "flow_first_state", "flow_last_state", "flow_leading_seconds",
    "flow_trailing_seconds", "flow_first_ts", "flow_last_ts",
    "flow_all_one_run", "flow_complete",
)


def _rollup_has_cluster_schema(roll):
    """True only for a complete v2 telemetry shape, never for a v1 default."""
    if not isinstance(roll, dict):
        return False
    version = _number(roll.get("v"))
    cluster = roll.get("cluster")
    return (version is not None and version >= ROLLUP_VERSION
            and isinstance(cluster, dict)
            and all(key in cluster for key in _CLUSTER_REQUIRED_KEYS))


def _rollup_has_device_telemetry(roll):
    if not _rollup_has_cluster_schema(roll):
        return False
    return (_number((roll.get("cluster") or {}).get("util_weight")) or 0.0) > 0


def _legacy_rebuild(raw_path):
    """Build one raw day once per unchanged source file in this process.

    The lock intentionally covers the cold build.  This path is only for a
    first v1 -> v2 upgrade; serializing it prevents a burst of /stats requests
    from duplicating decompression/accounting and guarantees every caller gets
    an equivalent complete rollup.
    """
    try:
        stat = os.stat(raw_path)
        key = (os.path.abspath(raw_path), stat.st_size, stat.st_mtime)
    except OSError:
        return build_rollup(raw_path)
    with _LEGACY_REBUILD_CACHE_LOCK:
        cached = _LEGACY_REBUILD_CACHE.get(key)
        if cached is not None:
            return cached
        rebuilt = build_rollup(raw_path)
        if len(_LEGACY_REBUILD_CACHE) >= _LEGACY_REBUILD_CACHE_MAX:
            _LEGACY_REBUILD_CACHE.clear()
        _LEGACY_REBUILD_CACHE[key] = rebuilt
        return rebuilt


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
    telemetry_dates_covered = []
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

        # v1 rollups cannot produce a truthful cluster pulse.  Rebuild them
        # lazily from the raw/gz history when it still exists, then atomically
        # persist v2 once.  If persistence is forbidden, the in-process cache
        # above avoids repeated decompression while retaining the old file.
        raw = None
        if roll is not None and not _rollup_has_cluster_schema(roll):
            raw = _raw_path_for(data_dir, date_str)
            if raw is not None:
                legacy_roll = roll
                try:
                    rebuilt = _legacy_rebuild(raw)
                    # A syntactically readable but fully corrupt/empty raw
                    # file must never erase a non-empty historical rollup.
                    # In that case retain the trusted v1 owner totals and
                    # omit pulse telemetry for this day.
                    rebuilt_samples = _number(rebuilt.get("samples")) or 0.0
                    legacy_samples = _number(legacy_roll.get("samples")) or 0.0
                    if rebuilt_samples > 0 or legacy_samples <= 0:
                        roll = rebuilt
                        _try_write_rollup(raw, roll)
                    else:
                        roll = legacy_roll
                except Exception:
                    # The old owner-only rollup remains useful even if a raw
                    # file has been corrupted since it was originally rolled.
                    try:
                        with open(rollup_path, "r", encoding="utf-8") as fh:
                            roll = json.load(fh)
                    except Exception:
                        roll = None

        if roll is None:
            raw = raw or _raw_path_for(data_dir, date_str)
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

        samples = _number((roll or {}).get("samples"))
        if roll is not None and (samples is None or samples >= 0):
            has_data = (samples or 0.0) > 0 or bool(roll.get("owners"))
            if has_data:
                filtered = _filter_owner(roll, owner)
                rollups.append(filtered)
                dates_covered.append(date_str)
                if owner is None and _rollup_has_device_telemetry(roll):
                    telemetry_dates_covered.append(date_str)
                # Reuse the per-day (owner-filtered) rollup we just loaded;
                # no second file read.
                day_owners = {}
                filtered_owners = filtered.get("owners") or {}
                if not isinstance(filtered_owners, dict):
                    filtered_owners = {}
                for own, acc in filtered_owners.items():
                    day_owners[own] = (_number(
                        acc.get("gpu_seconds") if isinstance(acc, dict)
                        else None) or 0.0)
                daily.append({
                    "date": date_str,
                    "owners": day_owners,
                    "coverage_seconds": filtered.get("coverage_seconds", 0.0),
                })

    merged = merge_rollups(rollups, stitch_flow=True)

    if not current_idle_done:
        current_idle = _current_idle(data_dir, today_str, owner, now_fn)

    awards = _awards_struct(
        merged.get("owners") or {},
        merged.get("pods_coverage_seconds", 0.0),
        merged.get("coverage_seconds", 0.0))

    insights = cluster_insights(merged)
    insights["telemetry_dates_covered"] = telemetry_dates_covered
    insights["available_dates_covered"] = list(dates_covered)
    insights["telemetry_coverage"] = {
        "covered": len(telemetry_dates_covered),
        "available": len(dates_covered),
        "unit": "days",
    }

    return {
        "window_days": days,
        "dates_covered": dates_covered,
        "merged": merged,
        "current_idle": current_idle,
        "daily": daily,
        "awards": awards,
        "insights": insights,
        "momentum": owner_momentum(daily, merged.get("owners") or {}),
        "telemetry_dates_covered": telemetry_dates_covered,
        "generated_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# --- lab-wide (multi-node) merge --------------------------------------------

def merge_query_results(labeled_results):
    """Merge N per-node query() results into one lab-wide result.

    Input: list of (node_label, query_result_dict) tuples, e.g.
    [("node-02", local), ("node-01", peer)]. Labels are short strings.
    Entries whose dict is falsy/malformed are skipped (their label is kept out
    of node_labels).

    Returns a dict with the SAME shape as query()'s return (so
    render_stats_text and JSON consumers work unchanged) plus lab-wide fields:
    "scope", "node_labels", "owner_nodes". The per-owner "owner_nodes" carries
    node affiliation so the report can show which node each person works on.
    """
    node_labels = []
    valid = []  # (label, result) pairs that contributed
    for label, res in labeled_results or []:
        if not res or not isinstance(res, dict):
            continue
        node_labels.append(label)
        valid.append((label, res))

    # merged: element-wise merge of each result's (rollup-shaped) "merged".
    merged = merge_rollups([res.get("merged") for _lbl, res in valid],
                           stitch_flow=False)

    # owner_nodes: {owner: {label: gpu_seconds}} from each merged.owners.
    owner_nodes = {}
    for label, res in valid:
        rmerged = res.get("merged") or {}
        for owner, acc in (rmerged.get("owners") or {}).items():
            owner_nodes.setdefault(owner, {})[label] = \
                acc.get("gpu_seconds", 0.0)

    # daily: union by date (oldest->newest). Per date, sum owners' gpu_seconds
    # across nodes; coverage_seconds = MAX across nodes for that date (parallel
    # monitors run concurrently, so their wall-clock coverage does not add).
    daily_by_date = {}   # date -> {"owners": {...}, "coverage_seconds": float}
    date_order = []
    for _label, res in valid:
        for d in res.get("daily") or []:
            date = d.get("date")
            if date is None:
                continue
            if date not in daily_by_date:
                daily_by_date[date] = {"owners": {}, "coverage_seconds": 0.0}
                date_order.append(date)
            slot = daily_by_date[date]
            for own, gs in (d.get("owners") or {}).items():
                slot["owners"][own] = slot["owners"].get(own, 0.0) + gs
            cov = d.get("coverage_seconds", 0.0)
            if cov > slot["coverage_seconds"]:
                slot["coverage_seconds"] = cov
    daily = []
    for date in sorted(date_order):
        slot = daily_by_date[date]
        daily.append({
            "date": date,
            "owners": slot["owners"],
            "coverage_seconds": slot["coverage_seconds"],
        })

    # current_idle: concatenation, tagging each entry with its node label.
    current_idle = []
    for label, res in valid:
        for entry in res.get("current_idle") or []:
            e = dict(entry)
            e["node"] = label
            current_idle.append(e)

    # awards: recomputed lab-wide over the merged owners.
    awards = _awards_struct(
        merged.get("owners") or {},
        merged.get("pods_coverage_seconds", 0.0),
        merged.get("coverage_seconds", 0.0))

    # window_days: max of inputs; dates_covered: sorted union; generated_utc:
    # max of inputs (ISO Z strings sort chronologically under string compare).
    window_days = 0
    dates_covered = set()
    generated_utc = ""
    telemetry_dates_covered = set()
    telemetry_covered_partitions = 0
    telemetry_available_partitions = 0
    for _label, res in valid:
        wd = res.get("window_days", 0) or 0
        if wd > window_days:
            window_days = wd
        for ds in res.get("dates_covered") or []:
            dates_covered.add(ds)
        for ds in res.get("telemetry_dates_covered") or []:
            telemetry_dates_covered.add(ds)
        coverage = (res.get("insights") or {}).get("telemetry_coverage")
        if isinstance(coverage, dict):
            covered = _number(coverage.get("covered"))
            available = _number(coverage.get("available"))
        else:
            covered = available = None
        # Peers running an older server do not carry the scalar coverage
        # shape. Their date arrays still describe one node's day coverage.
        telemetry_covered_partitions += int(
            covered if covered is not None and covered >= 0
            else len(res.get("telemetry_dates_covered") or []))
        telemetry_available_partitions += int(
            available if available is not None and available >= 0
            else len(res.get("dates_covered") or []))
        gu = res.get("generated_utc") or ""
        if gu > generated_utc:
            generated_utc = gu

    insights = cluster_insights(merged)
    insights["telemetry_dates_covered"] = sorted(telemetry_dates_covered)
    insights["available_dates_covered"] = sorted(dates_covered)
    insights["telemetry_coverage"] = {
        # A date union is insufficient here: node-01 and node-02 can have
        # different telemetry availability on the same UTC date.
        "covered": telemetry_covered_partitions,
        "available": telemetry_available_partitions,
        "unit": "node-days",
    }

    return {
        "window_days": window_days,
        "dates_covered": sorted(dates_covered),
        "merged": merged,
        "current_idle": current_idle,
        "daily": daily,
        "awards": awards,
        "insights": insights,
        "momentum": owner_momentum(daily, merged.get("owners") or {}),
        "telemetry_dates_covered": sorted(telemetry_dates_covered),
        "generated_utc": generated_utc,
        "scope": "lab",
        "node_labels": node_labels,
        "owner_nodes": owner_nodes,
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


def _safe_avg(wsum, weight):
    """Numeric-only weighted average for optional/older rollup fields."""
    wsum = _number(wsum)
    weight = _number(weight)
    if wsum is None or weight is None or weight <= 0:
        return None
    return wsum / weight


def cluster_insights(merged):
    """Return presentation-ready cluster telemetry from a merged rollup.

    This is intentionally a pure derived view: it costs no raw-file reads and
    safely returns ``has_device_telemetry=False`` for rollup v1 files.  Values
    are percentages except for the explicitly named seconds/hours fields.
    """
    cluster = (merged or {}).get("cluster") or {}
    util_avg = _safe_avg(cluster.get("util_wsum"), cluster.get("util_weight"))
    vram_avg = _safe_avg(cluster.get("mem_wsum"), cluster.get("mem_weight"))

    def hourly(sum_key, weight_key):
        sums = cluster.get(sum_key) or []
        weights = cluster.get(weight_key) or []
        return [_safe_avg(sums[i] if i < len(sums) else None,
                          weights[i] if i < len(weights) else None)
                for i in range(24)]

    raw_hist = cluster.get("util_hist") or []
    hist = []
    for i in range(UTIL_BUCKETS):
        value = _number(raw_hist[i] if i < len(raw_hist) else 0.0)
        hist.append(value if value is not None and value > 0 else 0.0)
    hist_total = sum(hist)

    def share(start, end):
        if hist_total <= 0:
            return None
        return 100.0 * sum(hist[start:end]) / hist_total

    return {
        "has_device_telemetry": util_avg is not None,
        "has_flow": bool(cluster.get("flow_complete"))
        and util_avg is not None,
        "flow_scope": cluster.get("flow_scope") or "device",
        "observed_gpu_hours": (_number(cluster.get("device_seconds")) or 0.0)
        / 3600.0,
        "util_avg": util_avg,
        "vram_avg": vram_avg,
        "util_by_hour_kst": hourly("hour_util_wsum", "hour_util_weight"),
        "vram_by_hour_kst": hourly("hour_mem_wsum", "hour_mem_weight"),
        # Four readable bands, while the underlying rollup retains 10 buckets.
        "util_bands": {
            "quiet": share(0, 1),       # 0-9%
            "light": share(1, 4),       # 10-39%
            "work": share(4, 7),        # 40-69%
            "hot": share(7, 10),        # 70-100%
        },
        "busy_windows": int(_number(cluster.get("busy_windows")) or 0),
        "busy_seconds": _number(cluster.get("busy_seconds")) or 0.0,
        "idle_windows": int(_number(cluster.get("idle_windows")) or 0),
        "idle_seconds": _number(cluster.get("idle_seconds")) or 0.0,
        "longest_busy_seconds": _number(
            cluster.get("longest_busy_seconds")) or 0.0,
        "longest_idle_seconds": _number(
            cluster.get("longest_idle_seconds")) or 0.0,
    }


def owner_momentum(daily, owners=None):
    """Streak and consistency per owner over days with actual observations.

    ``daily`` intentionally omits days with no samples.  Calling this
    consistency *observed-day consistency* avoids treating monitor downtime as
    a person's inactive day.  A current streak ends at the newest observed
    date, not necessarily the wall-clock date.
    """
    days = [entry for entry in (daily or []) if isinstance(entry, dict)
            and entry.get("date")]
    names = set((owners or {}).keys())
    for entry in days:
        names.update((entry.get("owners") or {}).keys())
    observed = len(days)
    out = []
    for owner in names:
        activity = []
        for entry in days:
            value = _number((entry.get("owners") or {}).get(owner)) or 0.0
            activity.append(value > 0)
        active_days = sum(1 for active in activity if active)
        longest = run = 0
        for active in activity:
            run = run + 1 if active else 0
            if run > longest:
                longest = run
        current = 0
        for active in reversed(activity):
            if not active:
                break
            current += 1
        acc = (owners or {}).get(owner) or {}
        out.append({
            "owner": owner,
            "active_days": active_days,
            "observed_days": observed,
            "consistency_pct": (100.0 * active_days / observed
                                if observed else 0.0),
            "current_streak_days": current,
            "longest_streak_days": longest,
            "gpu_seconds": _number(acc.get("gpu_seconds")) or 0.0,
            "efficiency_pct": _eff_util(acc),
        })
    out.sort(key=lambda item: (-item["current_streak_days"],
                               -item["consistency_pct"],
                               -item["gpu_seconds"], str(item["owner"])))
    return out


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


def _owner_node_label(owner, owner_nodes):
    """Which node an owner belongs to for the NODE column.

    Returns the single label if >= 95% of that owner's summed gpu_seconds is on
    one node, else "both"; "" when the owner is missing from owner_nodes.
    """
    per_node = (owner_nodes or {}).get(owner)
    if not per_node:
        return ""
    total = sum(per_node.values())
    if total <= 0:
        return ""
    top_label, top_gs = max(per_node.items(), key=lambda kv: kv[1])
    if top_gs / total >= 0.95:
        return top_label
    return "both"


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


def _format_duration(seconds):
    """Compact, deterministic duration for terminal insight lines."""
    seconds = max(0, int(round(_number(seconds) or 0.0)))
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    if hours:
        return "%dh%02dm" % (hours, minutes)
    if minutes:
        return "%dm" % minutes
    return "%ds" % seconds


def _percentage_spark(values, max_cells, unicode_ok):
    """Fixed-range 0..100% sparkline, downsampled without dependencies."""
    values = list(values or [])
    if max_cells < 1:
        return ""
    if len(values) > max_cells:
        reduced = []
        for cell in range(max_cells):
            start = int(cell * len(values) / float(max_cells))
            end = max(start + 1, int((cell + 1) * len(values) / float(max_cells)))
            nums = [_percent(v) for v in values[start:end]]
            nums = [v for v in nums if v is not None]
            reduced.append(sum(nums) / len(nums) if nums else None)
        values = reduced
    glyphs = " ▁▂▃▄▅▆▇█" if unicode_ok else " .:-=+*#"
    top = len(glyphs) - 1
    out = []
    for value in values:
        value = _percent(value)
        if value is None:
            out.append(" ")
        else:
            out.append(glyphs[int(round(value * top / 100.0))])
    return "".join(out)


def _pct_text(value):
    return "--" if value is None else "%d%%" % int(round(value))


def _telemetry_coverage(insights):
    """Return ``(partial, label)`` without losing LAB node-day coverage."""
    coverage = (insights or {}).get("telemetry_coverage")
    if isinstance(coverage, dict):
        covered = _number(coverage.get("covered"))
        available = _number(coverage.get("available"))
        if (covered is not None and available is not None
                and covered >= 0 and available >= 0):
            unit = str(coverage.get("unit") or "days")
            return covered < available, "%d/%d %s" % (
                int(covered), int(available), unit)
    telemetry_dates = (insights or {}).get("telemetry_dates_covered") or []
    available_dates = (insights or {}).get("available_dates_covered") or []
    return (bool(available_dates) and len(telemetry_dates) < len(available_dates),
            "%d/%d days" % (len(telemetry_dates), len(available_dates)))


def _render_cluster_pulse(lines, result, width, color, unicode_ok):
    """Append the compact cluster telemetry hierarchy when v2 data exists."""
    insights = result.get("insights")
    if not isinstance(insights, dict):
        insights = cluster_insights(result.get("merged") or {})
    if not insights.get("has_device_telemetry"):
        return False

    util = insights.get("util_avg")
    vram = insights.get("vram_avg")
    bands = insights.get("util_bands") or {}
    hot = bands.get("hot")
    partial, coverage_label = _telemetry_coverage(insights)
    pulse_title = "Cluster pulse"
    if partial:
        pulse_title += " (partial telemetry: %s)" % coverage_label
    lines.append(_c("bold", pulse_title, color))

    # Wide terminals get two 24-hour KST sparklines.  Keep the labels and
    # headline figures intact on smaller terminals rather than wrapping a
    # graph into an unreadable shape.
    if width >= 76:
        spark_cells = min(24, max(12, width - 50))
        util_spark = _percentage_spark(insights.get("util_by_hour_kst"),
                                       spark_cells, unicode_ok)
        vram_spark = _percentage_spark(insights.get("vram_by_hour_kst"),
                                       spark_cells, unicode_ok)
        lines.append("KST  UTIL %-*s  avg %s  hot %s" % (
            spark_cells, util_spark, _pct_text(util), _pct_text(hot)))
        lines.append("     VRAM %-*s  avg %s" % (
            spark_cells, vram_spark, _pct_text(vram)))
        lines.append("UTIL mix  quiet %s  light %s  work %s  hot %s" % (
            _pct_text(bands.get("quiet")), _pct_text(bands.get("light")),
            _pct_text(bands.get("work")), _pct_text(hot)))
        busy_windows = insights.get("busy_windows", 0)
        longest = insights.get("longest_busy_seconds", 0.0)
        if insights.get("has_flow") and busy_windows:
            if insights.get("flow_scope") == "node":
                flow_line = ("Flow      %d node compute windows  ·  longest %s "
                             "per-node (any GPU >= %d%%)" % (
                                 busy_windows, _format_duration(longest),
                                 BUSY_UTIL_THRESHOLD))
            else:
                flow_line = ("Flow      %d compute windows  ·  longest %s "
                             "(any GPU >= %d%%)" % (
                                 busy_windows, _format_duration(longest),
                                 BUSY_UTIL_THRESHOLD))
            lines.append(_c("dim", flow_line, color))
    elif width >= 54:
        spark_cells = min(18, max(8, width - 34))
        spark = _percentage_spark(insights.get("util_by_hour_kst"),
                                  spark_cells, unicode_ok)
        lines.append("KST UTIL %-*s  avg %s" %
                     (spark_cells, spark, _pct_text(util)))
        lines.append("VRAM %s  hot %s  %d windows" % (
            _pct_text(vram), _pct_text(hot),
            insights.get("busy_windows", 0)))
    else:
        lines.append("util %s  vram %s  hot %s" %
                     (_pct_text(util), _pct_text(vram), _pct_text(hot)))
    return True


def _render_momentum(lines, result, width, color):
    """Append one focused owner-streak line below the leaderboard."""
    momentum = result.get("momentum")
    if not isinstance(momentum, list):
        merged = result.get("merged") or {}
        momentum = owner_momentum(result.get("daily") or [],
                                   merged.get("owners") or {})
    eligible = [row for row in momentum if isinstance(row, dict)
                and row.get("observed_days", 0) >= 2
                and row.get("active_days", 0) > 0]
    if not eligible:
        return False

    lines.append(_c("bold", "Momentum", color))
    if width < 58:
        row = eligible[0]
        lines.append("%s  %dd streak  ·  %d/%d active observed days" % (
            _clip(row.get("owner"), 14), row.get("current_streak_days", 0),
            row.get("active_days", 0), row.get("observed_days", 0)))
        return True

    parts = []
    for row in eligible[:3]:
        eff = row.get("efficiency_pct")
        eff_text = "" if eff is None else " · %d%% eff" % int(round(eff))
        part = "%s %dd streak · %d/%dd%s" % (
            _clip(row.get("owner"), 14), row.get("current_streak_days", 0),
            row.get("active_days", 0), row.get("observed_days", 0), eff_text)
        candidate = "  |  ".join(parts + [part])
        if parts and len(candidate) > width:
            break
        parts.append(part)
    lines.append("  |  ".join(parts))
    lines.append(_c("dim", "streaks and consistency use observed days only", color))
    return True


def _awards_struct(owners, pods_cov, coverage_seconds):
    """Build the structured awards list (dicts) from owner accumulators.

    Structured awards so JSON consumers (the TUI stats view) don't have to
    duplicate the threshold logic. Text format is "Title: owner — detail".
    Split out so the lab merge can recompute awards over merged owners.
    """
    awards = []
    for key, own, text in _compute_awards(owners, pods_cov, coverage_seconds):
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
    return awards


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
    owner_nodes = result.get("owner_nodes")

    lines = []
    rule_char = "─" if unicode_ok else "-"

    def rule():
        lines.append(_c("dim", rule_char * max(1, width), color))

    # --- a. title + subtitle ---
    if result.get("scope") == "lab":
        node_labels = result.get("node_labels") or []
        title = ("SGPU usage report — last %d days — all nodes (%s)"
                 % (window_days, "+".join(node_labels)))
    else:
        title = "SGPU usage report — last %d days" % window_days
    lines.append(_c("cyan", _c("bold", title, color), color))
    day_word = "day" if len(dates) == 1 else "days"
    subtitle = "data: %d %s, coverage %.1fh" % (len(dates), day_word, coverage_h)
    lines.append(_c("dim", subtitle, color))
    rule()

    if not owners and merged.get("samples", 0) == 0:
        lines.append("no samples recorded yet")
        return "\n".join(lines) + "\n"

    if _render_cluster_pulse(lines, result, width, color, unicode_ok):
        rule()

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
        rule()

    # --- c. leaderboard ---
    # When the result carries per-owner node affiliation (lab merge), add a
    # NODE column right after OWNER so the report shows which node each person
    # works on. Its width fits the labels (min 4 for the "NODE"/"both" header).
    show_node = owner_nodes is not None
    if show_node:
        nw = max([4] + [len(_owner_node_label(o, owner_nodes)) for o in owners])
    lines.append(_c("bold", "Leaderboard", color))
    if show_node:
        header = ("%-3s %-*s %-*s %6s %6s %8s %10s %9s %8s %8s %6s"
                  % ("#", ow, "OWNER", nw, "NODE", "GPU-H", "EFF-H", "AVG-SM%",
                     "AVG-UTIL%", "PEAK-MEM", "ALLOC-H", "IDLE-H", "IDLE%"))
    else:
        header = ("%-3s %-*s %6s %6s %8s %10s %9s %8s %8s %6s"
                  % ("#", ow, "OWNER", "GPU-H", "EFF-H", "AVG-SM%", "AVG-UTIL%",
                     "PEAK-MEM", "ALLOC-H", "IDLE-H", "IDLE%"))
    lines.append(_c("bold", header, color))

    for rank, (owner, acc) in enumerate(ranked, start=1):
        rank_s = "%d." % rank
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

        if show_node:
            node_s = _owner_node_label(owner, owner_nodes)
            row = ("%-3s %-*s %-*s %6s %6s %8s %10s %9s %8s %8s %6s"
                   % (rank_s, ow, _clip(owner, ow), nw, node_s, "%.1f" % gpu_h,
                      eff_s, sm_s, util_s, peak_s, alloc_s, idle_s, idlep_s))
        else:
            row = ("%-3s %-*s %6s %6s %8s %10s %9s %8s %8s %6s"
                   % (rank_s, ow, _clip(owner, ow), "%.1f" % gpu_h, eff_s, sm_s,
                      util_s, peak_s, alloc_s, idle_s, idlep_s))
        lines.append(row)

    if pods_cov == 0:
        lines.append(_c("dim",
                        "(allocation stats unavailable: no pod API coverage)",
                        color))

    _render_momentum(lines, result, width, color)
    rule()

    # --- d. daily activity calendar (the "grass") ---
    if len(dates) >= 2:
        _render_grass(lines, ranked, daily, dates, ow, color, unicode_ok, width)
        rule()

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
    rule()

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
        node = entry.get("node")
        gpu_part = ("%d GPU, %s" % (entry.get("req", 0), node) if node
                    else "%d GPU" % entry.get("req", 0))
        warnings.append("IDLE-NOW pod %s (%s) idle %dm"
                        % (entry.get("pod"), gpu_part,
                           entry.get("idle_minutes", 0)))

    if warnings:
        lines.append(_c("bold", "Warnings", color))
        for w in warnings:
            lines.append(_c("yellow", "  " + w, color))
        lines.append("")

    # --- g. notes (e.g. server-added "node-01 unreachable: ...") ---
    for note in result.get("notes") or []:
        lines.append(_c("dim", note, color))

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

    # --- legend (blank line below the grid so it doesn't blur into it) ---
    lines.append("")
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
