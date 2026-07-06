"""statsdb: raw sample recording and daily rotation for the sgpu stats subsystem.

Invariants
----------
- Single-writer: only the sampler thread writes sample lines and performs
  rotation. NVML thread-safety is entirely collect_fn's concern; this module
  never touches NVML.
- Raw fidelity: every sample line preserves every numeric field the collector
  produced so future analysis programs can recompute new statistics. We never
  average or drop values at write time.
- Crash-safety: each line is a complete JSON object written and flushed on its
  own. A process killed mid-write leaves at most one partial trailing line,
  which readers skip. Same-day files are always APPENDED to (never truncated),
  so a pod restart mid-day keeps prior samples.
- Never kill the server: the sampler loop catches every exception per cycle,
  prints a one-line error and keeps looping.

Files live under the resolved data dir:
    samples-YYYYMMDD.jsonl        today's raw samples (UTC date)
    samples-YYYYMMDD.jsonl.gz     compressed past days
    rollup-YYYYMMDD.json          daily aggregate written by statsagg

This module is stdlib-only and importable on Windows (no fcntl etc.), though
production runs on Linux.
"""

import gzip
import os
import shutil
import threading
import time
from datetime import datetime, timezone

import statsagg


DEFAULT_DATA_DIR = "/var/lib/sgpu"
FALLBACK_DATA_DIR = "/tmp/sgpu-data"
DEFAULT_INTERVAL = 15
DEFAULT_RETENTION_DAYS = 365
DEFAULT_MAX_DATA_MB = 2048

CMD_MAX = 120

# Module state describing the running sampler. Populated by start_sampler.
_STATE = {
    "data_dir": None,     # directory actually in use (after fallback resolution)
    "requested_dir": None,
    "fallback": False,
    "interval": DEFAULT_INTERVAL,
    "retention_days": DEFAULT_RETENTION_DAYS,
    "max_data_mb": DEFAULT_MAX_DATA_MB,
    "sampling": False,
    "thread": None,
}
_LOCK = threading.Lock()


def _utcnow():
    return datetime.now(timezone.utc)


def _iso_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _date_str(dt):
    return dt.strftime("%Y%m%d")


def _trunc(value, limit):
    if value is None:
        return None
    text = str(value)
    if len(text) > limit:
        return text[:limit]
    return text


def snapshot_to_sample(snapshot):
    """Convert a collector schema-2 snapshot into a compact v1 sample record.

    Pure function (exposed for tests). None values pass through as null. The
    process command is truncated to CMD_MAX chars. The "pods" key is present
    only when snapshot["pods"]["ok"] is true.
    """
    gpus = []
    for g in snapshot.get("gpus", []) or []:
        gpus.append({
            "i": g.get("index"),
            "uuid": g.get("uuid"),
            "util": g.get("util"),
            "mem": g.get("mem_used_mib"),
            "mem_total": g.get("mem_total_mib"),
            "temp": g.get("temp_c"),
            "pw": g.get("power_w"),
            "pw_lim": g.get("power_limit_w"),
        })

    procs = []
    for p in snapshot.get("procs", []) or []:
        procs.append({
            "pid": p.get("pid"),
            "gpu": p.get("gpu_index"),
            "mem": p.get("mem_mib"),
            "owner": p.get("owner"),
            "pod": p.get("pod"),
            "cmd": _trunc(p.get("cmd"), CMD_MAX),
            "started": p.get("started_utc"),
            "sm": p.get("sm_util"),
        })

    sample = {
        "v": 1,
        "ts": snapshot.get("time_utc"),
        "node": snapshot.get("node"),
        "driver": snapshot.get("driver"),
        "gpus": gpus,
        "procs": procs,
    }

    pods = snapshot.get("pods") or {}
    if pods.get("ok"):
        rows = []
        for r in pods.get("rows", []) or []:
            rows.append({
                "pod": r.get("pod"),
                "owner": r.get("owner"),
                "req": r.get("gpu"),
                "phase": r.get("phase"),
                "start": r.get("start_iso"),
            })
        sample["pods"] = rows

    return sample


def _dir_writable(path):
    """True if path exists as a dir we can create files in, or can be created."""
    try:
        if not os.path.isdir(path):
            os.makedirs(path, exist_ok=True)
        # Probe for actual write access; makedirs can succeed on a path that is
        # then not writable, and an existing dir may be read-only.
        probe = os.path.join(path, ".sgpu-write-test")
        with open(probe, "w", encoding="utf-8") as fh:
            fh.write("")
        os.remove(probe)
        return True
    except Exception:
        return False


def _resolve_data_dir(requested):
    """Return (dir, fallback_bool). Fall back to FALLBACK_DATA_DIR if needed."""
    if _dir_writable(requested):
        return requested, False
    if _dir_writable(FALLBACK_DATA_DIR):
        return FALLBACK_DATA_DIR, True
    # Last resort: still report the fallback path even if it is not writable,
    # so status() reflects the degraded intent. Writes will fail and be caught.
    return FALLBACK_DATA_DIR, True


def resolved_data_dir():
    """The directory actually in use, or None before start_sampler."""
    with _LOCK:
        return _STATE["data_dir"]


def status():
    """Snapshot dict for /version and the dashboard footer.

    Works sensibly even if the sampler never started (data_dir None -> zeros).
    """
    with _LOCK:
        data_dir = _STATE["data_dir"]
        fallback = _STATE["fallback"]
        sampling = _STATE["sampling"]
        interval = _STATE["interval"]

    files = 0
    total_bytes = 0
    writable = False
    if data_dir:
        writable = _dir_writable(data_dir)
        try:
            for name in os.listdir(data_dir):
                if not (name.startswith("samples-") or name.startswith("rollup-")):
                    continue
                full = os.path.join(data_dir, name)
                if os.path.isfile(full):
                    files += 1
                    total_bytes += os.path.getsize(full)
        except Exception:
            pass

    return {
        "data_dir": data_dir,
        "writable": writable,
        "fallback": bool(fallback),
        "files": files,
        "total_mb": round(total_bytes / (1024 * 1024), 1),
        "sampling": bool(sampling),
        "interval": interval,
    }


def _sample_path(data_dir, dt):
    return os.path.join(data_dir, "samples-%s.jsonl" % _date_str(dt))


def _sample_once(collect_fn, data_dir, now_fn=_utcnow):
    """Collect one snapshot and append it as one JSON line to today's file.

    Returns the path written. Exposed for direct unit testing (no sleeping).
    """
    import json  # local import keeps module import cheap and explicit

    snap = collect_fn(force=True)
    sample = snapshot_to_sample(snap)
    now = now_fn()
    path = _sample_path(data_dir, now)
    line = json.dumps(sample, ensure_ascii=False, separators=(",", ":"))
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line)
        fh.write("\n")
        fh.flush()
    return path


def _gzip_file(src, dst):
    """Gzip src -> dst, remove src only after dst is fully written."""
    with open(src, "rb") as fin:
        with gzip.open(dst, "wb") as fout:
            shutil.copyfileobj(fin, fout)
    os.remove(src)


def _enforce_retention(data_dir, retention_days, now_fn=_utcnow):
    """Delete samples-*.jsonl.gz whose date is older than retention_days."""
    now = now_fn()
    cutoff = now.timestamp() - retention_days * 86400
    for name in os.listdir(data_dir):
        if not (name.startswith("samples-") and name.endswith(".jsonl.gz")):
            continue
        date_str = name[len("samples-"):-len(".jsonl.gz")]
        try:
            d = datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if d.timestamp() < cutoff:
            try:
                os.remove(os.path.join(data_dir, name))
            except Exception:
                pass


def _enforce_size_cap(data_dir, max_data_mb, now_fn=_utcnow):
    """If total samples-* size exceeds cap, delete oldest .gz first.

    Never deletes today's raw file.
    """
    today_raw = "samples-%s.jsonl" % _date_str(now_fn())
    cap_bytes = max_data_mb * 1024 * 1024

    def sample_files():
        out = []
        for name in os.listdir(data_dir):
            if not name.startswith("samples-"):
                continue
            full = os.path.join(data_dir, name)
            if os.path.isfile(full):
                out.append((name, full, os.path.getsize(full)))
        return out

    files = sample_files()
    total = sum(size for _, _, size in files)
    if total <= cap_bytes:
        return

    # Oldest first by name (samples-YYYYMMDD... sorts chronologically). Only
    # compressed past-day files are eligible; never the current raw file.
    gz = sorted(
        (f for f in files if f[0].endswith(".jsonl.gz")),
        key=lambda f: f[0],
    )
    for name, full, size in gz:
        if total <= cap_bytes:
            break
        if name == today_raw:
            continue
        try:
            os.remove(full)
            total -= size
        except Exception:
            pass


def _rotate(data_dir, date_str, retention_days, max_data_mb, now_fn=_utcnow):
    """Rotate one past day: gzip its raw file, write rollup, enforce limits.

    date_str is the YYYYMMDD of the day being rotated (yesterday, or any past
    day found on startup). Safe to call when the raw file is absent (only the
    .gz exists) — it still (re)builds the rollup and enforces limits.
    Exposed for direct unit testing.
    """
    raw = os.path.join(data_dir, "samples-%s.jsonl" % date_str)
    gz = os.path.join(data_dir, "samples-%s.jsonl.gz" % date_str)

    if os.path.isfile(raw):
        try:
            _gzip_file(raw, gz)
        except Exception as exc:
            print("statsdb: gzip failed for %s: %s" % (raw, exc), flush=True)

    if os.path.isfile(gz):
        try:
            statsagg.write_rollup(gz)
        except Exception as exc:
            print("statsdb: rollup failed for %s: %s" % (gz, exc), flush=True)

    try:
        _enforce_retention(data_dir, retention_days, now_fn=now_fn)
    except Exception as exc:
        print("statsdb: retention failed: %s" % exc, flush=True)
    try:
        _enforce_size_cap(data_dir, max_data_mb, now_fn=now_fn)
    except Exception as exc:
        print("statsdb: size cap failed: %s" % exc, flush=True)


def _backfill(data_dir, retention_days, max_data_mb, now_fn=_utcnow):
    """On startup, rotate any samples-*.jsonl whose date is in the past.

    Covers pod restarts mid-day: today's file is left alone (it will be
    appended to). Runs synchronously at startup before the loop begins.
    """
    today = _date_str(now_fn())
    try:
        names = os.listdir(data_dir)
    except Exception:
        return
    for name in sorted(names):
        if not (name.startswith("samples-") and name.endswith(".jsonl")):
            continue
        date_str = name[len("samples-"):-len(".jsonl")]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        if date_str >= today:
            continue
        _rotate(data_dir, date_str, retention_days, max_data_mb, now_fn=now_fn)


def _spawn_rotate(data_dir, date_str, retention_days, max_data_mb, now_fn):
    t = threading.Thread(
        target=_rotate,
        args=(data_dir, date_str, retention_days, max_data_mb),
        kwargs={"now_fn": now_fn},
        daemon=True,
        name="sgpu-rotate-%s" % date_str,
    )
    t.start()
    return t


def _run_loop(collect_fn, data_dir, interval, retention_days, max_data_mb,
              now_fn, sleep_fn, stop_event):
    """Sampler loop. Catches all per-cycle exceptions; never raises out."""
    current_day = _date_str(now_fn())
    while not (stop_event and stop_event.is_set()):
        try:
            now = now_fn()
            day = _date_str(now)
            if day != current_day:
                # UTC date rollover: rotate the day that just ended in a
                # one-shot background thread so the loop keeps sampling.
                _spawn_rotate(data_dir, current_day, retention_days,
                              max_data_mb, now_fn)
                current_day = day
            _sample_once(collect_fn, data_dir, now_fn=now_fn)
        except Exception as exc:
            print("statsdb: sample cycle error: %s: %s"
                  % (type(exc).__name__, exc), flush=True)
        if stop_event and stop_event.is_set():
            break
        try:
            sleep_fn(interval)
        except Exception:
            break


def start_sampler(collect_fn, data_dir=None, interval=None, retention_days=None,
                  max_data_mb=None, now_fn=None, sleep_fn=None, stop_event=None):
    """Resolve the data dir, backfill past days, and start the sampler thread.

    Env vars (read here unless overridden by kwargs):
      SGPU_DATA_DIR         default /var/lib/sgpu
      SGPU_SAMPLE_INTERVAL  default 15 seconds
      SGPU_RETENTION_DAYS   default 365
      SGPU_MAX_DATA_MB      default 2048

    Returns the daemon thread. now_fn/sleep_fn/stop_event/data_dir are injectable
    so tests can run without real sleeping.
    """
    if data_dir is None:
        data_dir = os.environ.get("SGPU_DATA_DIR", DEFAULT_DATA_DIR)
    if interval is None:
        interval = int(os.environ.get("SGPU_SAMPLE_INTERVAL", DEFAULT_INTERVAL))
    if retention_days is None:
        retention_days = int(os.environ.get("SGPU_RETENTION_DAYS",
                                            DEFAULT_RETENTION_DAYS))
    if max_data_mb is None:
        max_data_mb = int(os.environ.get("SGPU_MAX_DATA_MB", DEFAULT_MAX_DATA_MB))
    if now_fn is None:
        now_fn = _utcnow
    if sleep_fn is None:
        sleep_fn = time.sleep

    resolved, fallback = _resolve_data_dir(data_dir)

    with _LOCK:
        _STATE["requested_dir"] = data_dir
        _STATE["data_dir"] = resolved
        _STATE["fallback"] = fallback
        _STATE["interval"] = interval
        _STATE["retention_days"] = retention_days
        _STATE["max_data_mb"] = max_data_mb
        _STATE["sampling"] = True

    if fallback:
        print("statsdb: data dir %r not usable, falling back to %r"
              % (data_dir, resolved), flush=True)

    try:
        _backfill(resolved, retention_days, max_data_mb, now_fn=now_fn)
    except Exception as exc:
        print("statsdb: backfill error: %s" % exc, flush=True)

    thread = threading.Thread(
        target=_run_loop,
        args=(collect_fn, resolved, interval, retention_days, max_data_mb,
              now_fn, sleep_fn, stop_event),
        daemon=True,
        name="sgpu-sampler",
    )
    with _LOCK:
        _STATE["thread"] = thread
    thread.start()
    return thread
