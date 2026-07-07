"""GPU snapshot collection for sgpu.

Produces the schema-2 snapshot consumed by server.py, tui.py and statsdb.py:
NVML (preferred, same libnvidia-ml the image pins) with an nvidia-smi CSV
fallback, plus /proc-based process -> pod attribution (the pod runs with
hostPID, so compute PIDs are host PIDs and their /proc entries are visible).

Invariants:
- Every NVML call happens while holding _lock (HTTP threads and the stats
  sampler share this module).
- The attribution cache is keyed by (pid, starttime ticks) so a recycled PID
  can never inherit a stale pod name.
- SGPU_MOCK=1 swaps in a synthetic snapshot; nothing touches NVML or /proc,
  so the whole pipeline is testable on machines without GPUs.
"""

import csv
import math
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone

SGPU_VERSION = "0.8.1"
SCHEMA = 2

MOCK = os.environ.get("SGPU_MOCK", "") == "1"
NODE_NAME = os.environ.get("NODE_NAME", "")
CACHE_TTL = float(os.environ.get("GPU_MONITOR_CACHE_TTL", "1.0"))
ENVIRON_READ_CAP = 1024 * 1024
POD_ENV_KEYS = ("HOSTNAME", "POD_NAME", "MY_POD_NAME", "K8S_POD_NAME")
_CGROUP_POD_RE = re.compile(
    r"pod([0-9a-fA-F]{8}[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{4}"
    r"[-_][0-9a-fA-F]{4}[-_][0-9a-fA-F]{12})"
)

_lock = threading.RLock()
_cache = {"at": 0.0, "data": None}
_pynvml = None            # module handle once nvmlInit succeeded
_nvml_error = None        # first init failure, reported via /version
_uuid_index = {}          # GPU UUID -> device index
_attr_cache = {}          # (pid, starttime ticks) -> attribution dict
_proc_util_ts = {}        # device index -> lastSeenTimeStamp for sm samples
_sm_by_pid = {}           # pid -> most recent smUtil
_btime = None             # host boot time (epoch seconds), cached


def owner_from_name(name):
    if not name:
        return None
    return name.replace("_", "-").split("-", 1)[0]


# --- /proc attribution -------------------------------------------------------


def _read_environ_map(pid):
    try:
        with open("/proc/%d/environ" % pid, "rb") as fh:
            raw = fh.read(ENVIRON_READ_CAP)
    except OSError:
        return {}
    found = {}
    for chunk in raw.split(b"\0"):
        if b"=" not in chunk:
            continue
        key, _, value = chunk.partition(b"=")
        try:
            key = key.decode("ascii")
        except UnicodeDecodeError:
            continue
        if key in POD_ENV_KEYS and key not in found:
            found[key] = value.decode("utf-8", "replace")
    return found


def _read_cmdline(pid):
    try:
        with open("/proc/%d/cmdline" % pid, "rb") as fh:
            raw = fh.read(65536)
    except OSError:
        return ""
    text = raw.replace(b"\0", b" ").decode("utf-8", "replace").strip()
    if text:
        return text
    try:
        with open("/proc/%d/comm" % pid) as fh:
            return fh.read().strip()
    except OSError:
        return ""


def _proc_stat_fields(pid):
    # /proc/<pid>/stat is "pid (comm) state ..." and comm may contain spaces
    # or parens, so split after the LAST ')'.
    try:
        with open("/proc/%d/stat" % pid) as fh:
            raw = fh.read()
    except OSError:
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    return raw[closing + 1:].split()


def _proc_starttime_ticks(pid):
    fields = _proc_stat_fields(pid)
    if not fields or len(fields) < 20:
        return None
    try:
        return int(fields[19])  # overall field 22: starttime
    except ValueError:
        return None


def _boot_time():
    global _btime
    if _btime is not None:
        return _btime
    try:
        with open("/proc/stat") as fh:
            for line in fh:
                if line.startswith("btime "):
                    _btime = int(line.split()[1])
                    return _btime
    except OSError:
        pass
    _btime = 0
    return _btime


def _iso_utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _proc_started_utc(starttime_ticks):
    boot = _boot_time()
    if not boot or starttime_ticks is None:
        return None
    tick = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
    return _iso_utc(boot + starttime_ticks / float(tick))


def _pod_uid_from_cgroup(pid):
    try:
        with open("/proc/%d/cgroup" % pid) as fh:
            raw = fh.read()
    except OSError:
        return None
    match = _CGROUP_POD_RE.search(raw)
    if not match:
        return None
    return match.group(1).replace("_", "-").lower()


def _attribute(pid, uid_to_pod):
    """Resolve pid -> pod/owner. Cached per (pid, starttime)."""
    ticks = _proc_starttime_ticks(pid)
    key = (pid, ticks)
    cached = _attr_cache.get(key)
    if cached is not None:
        # A uid-only entry can be upgraded once the pod API becomes available.
        if cached["pod"] is None and cached["pod_uid"] and uid_to_pod:
            pod = uid_to_pod.get(cached["pod_uid"])
            if pod:
                cached = dict(cached, pod=pod, owner=owner_from_name(pod),
                              attribution="cgroup")
                _attr_cache[key] = cached
        return cached

    cmd = _read_cmdline(pid)
    started = _proc_started_utc(ticks)
    env = _read_environ_map(pid)
    pod = None
    for env_key in ("POD_NAME", "MY_POD_NAME", "K8S_POD_NAME", "HOSTNAME"):
        if env.get(env_key):
            pod = env[env_key]
            break
    pod_uid = None
    method = "environ"
    if pod is None:
        pod_uid = _pod_uid_from_cgroup(pid)
        if pod_uid and uid_to_pod and uid_to_pod.get(pod_uid):
            pod = uid_to_pod[pod_uid]
            method = "cgroup"
        else:
            method = "none"
    entry = {
        "pod": pod,
        "owner": owner_from_name(pod),
        "pod_uid": pod_uid,
        "cmd": cmd,
        "started_utc": started,
        "attribution": method,
    }
    _attr_cache[key] = entry
    return entry


def _evict_attr_cache(live_pids):
    for key in [k for k in _attr_cache if k[0] not in live_pids]:
        del _attr_cache[key]


# --- NVML path ---------------------------------------------------------------


def _to_str(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _nvml():
    """Import + init pynvml once. Returns module or None."""
    global _pynvml, _nvml_error
    if _pynvml is not None:
        return _pynvml
    if _nvml_error is not None:
        return None
    try:
        import pynvml
        pynvml.nvmlInit()
        _pynvml = pynvml
        return _pynvml
    except Exception as exc:
        _nvml_error = "%s: %s" % (type(exc).__name__, exc)
        return None


def _nvml_call(func, *args, default=None):
    try:
        return func(*args)
    except Exception:
        return default


def _collect_nvml():
    nv = _nvml()
    if nv is None:
        return None
    try:
        count = nv.nvmlDeviceGetCount()
    except Exception:
        return None
    driver = _to_str(_nvml_call(nv.nvmlSystemGetDriverVersion, default="?"))
    gpus = []
    raw_procs = []
    _uuid_index.clear()
    for index in range(count):
        try:
            handle = nv.nvmlDeviceGetHandleByIndex(index)
        except Exception:
            continue
        uuid = _to_str(_nvml_call(nv.nvmlDeviceGetUUID, handle, default="?"))
        _uuid_index[uuid] = index
        util = _nvml_call(nv.nvmlDeviceGetUtilizationRates, handle)
        mem = _nvml_call(nv.nvmlDeviceGetMemoryInfo, handle)
        temp = _nvml_call(nv.nvmlDeviceGetTemperature, handle,
                          nv.NVML_TEMPERATURE_GPU)
        power = _nvml_call(nv.nvmlDeviceGetPowerUsage, handle)
        limit = _nvml_call(nv.nvmlDeviceGetEnforcedPowerLimit, handle)
        if limit is None:
            limit = _nvml_call(nv.nvmlDeviceGetPowerManagementLimit, handle)
        gpus.append({
            "index": index,
            "uuid": uuid,
            "name": _to_str(_nvml_call(nv.nvmlDeviceGetName, handle,
                                       default="?")),
            "util": util.gpu if util is not None else None,
            "mem_used_mib": (mem.used // (1024 * 1024)) if mem else None,
            "mem_total_mib": (mem.total // (1024 * 1024)) if mem else None,
            "temp_c": temp,
            "power_w": round(power / 1000.0, 1) if power is not None else None,
            "power_limit_w": round(limit / 1000.0, 1) if limit is not None else None,
        })
        for proc in _nvml_call(nv.nvmlDeviceGetComputeRunningProcesses, handle,
                               default=[]) or []:
            mem_mib = None
            if getattr(proc, "usedGpuMemory", None) is not None:
                mem_mib = proc.usedGpuMemory // (1024 * 1024)
            raw_procs.append({"pid": proc.pid, "gpu_index": index,
                              "gpu_uuid": uuid, "mem_mib": mem_mib})
        # Rolling per-process SM% samples; NOT_FOUND just means "nothing new".
        last_ts = _proc_util_ts.get(index, 0)
        samples = _nvml_call(nv.nvmlDeviceGetProcessUtilization, handle,
                             last_ts, default=[]) or []
        for sample in samples:
            _proc_util_ts[index] = max(_proc_util_ts.get(index, 0),
                                       sample.timeStamp)
            if 0 <= sample.smUtil <= 100:
                _sm_by_pid[sample.pid] = sample.smUtil
    return {"driver": driver, "gpus": gpus, "procs": raw_procs,
            "source": "nvml"}


# --- nvidia-smi fallback -----------------------------------------------------


GPU_FIELDS = ["index", "uuid", "name", "temperature.gpu", "utilization.gpu",
              "memory.total", "memory.used", "power.draw", "power.limit"]
APP_FIELDS = ["gpu_uuid", "pid", "used_memory"]


def _run(cmd, timeout=8):
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True,
                              timeout=timeout)
        return proc.returncode == 0, proc.stdout, proc.stderr
    except Exception as exc:
        return False, "", "%s: %s" % (type(exc).__name__, exc)


def _smi_num(value):
    value = (value or "").strip()
    if not value or value in ("[Not Supported]", "N/A", "[N/A]"):
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return None


def _collect_smi():
    ok, out, err = _run(["nvidia-smi",
                         "--query-gpu=" + ",".join(GPU_FIELDS),
                         "--format=csv,noheader,nounits"])
    if not ok:
        return {"driver": "?", "gpus": [], "procs": [], "source": "nvidia-smi",
                "error": err.strip() or "nvidia-smi query failed"}
    gpus = []
    _uuid_index.clear()
    for row in csv.reader(out.splitlines()):
        if len(row) < len(GPU_FIELDS):
            continue
        row = [cell.strip() for cell in row]
        index = _smi_num(row[0])
        if index is None:
            continue
        _uuid_index[row[1]] = index
        power = _smi_num(row[7])
        limit = _smi_num(row[8])
        gpus.append({
            "index": index, "uuid": row[1], "name": row[2],
            "temp_c": _smi_num(row[3]), "util": _smi_num(row[4]),
            "mem_total_mib": _smi_num(row[5]), "mem_used_mib": _smi_num(row[6]),
            "power_w": float(power) if power is not None else None,
            "power_limit_w": float(limit) if limit is not None else None,
        })
    procs = []
    ok, out, _ = _run(["nvidia-smi",
                       "--query-compute-apps=" + ",".join(APP_FIELDS),
                       "--format=csv,noheader,nounits"])
    if ok:
        for row in csv.reader(out.splitlines()):
            if len(row) < len(APP_FIELDS):
                continue
            row = [cell.strip() for cell in row]
            pid = _smi_num(row[1])
            if pid is None:
                continue
            procs.append({"pid": int(pid), "gpu_uuid": row[0],
                          "gpu_index": _uuid_index.get(row[0]),
                          "mem_mib": _smi_num(row[2])})
    driver = "?"
    ok, out, _ = _run(["nvidia-smi", "--query-gpu=driver_version",
                       "--format=csv,noheader"])
    if ok and out.strip():
        driver = out.strip().splitlines()[0].strip()
    return {"driver": driver, "gpus": gpus, "procs": procs,
            "source": "nvidia-smi"}


# --- mock mode ---------------------------------------------------------------


_MOCK_OWNERS = ("atlas", "nova", "orion")


def _mock_snapshot(now):
    gpus = []
    procs = []
    pods = []
    pid = 40000
    for index in range(8):
        phase = math.sin(now / 45.0 + index * 1.7)
        busy = index in (0, 3, 4, 5, 6)
        util = int(max(0, min(100, 78 + 20 * phase))) if busy else 0
        mem_total = 143771
        mem_used = int(mem_total * (0.45 + 0.12 * phase)) if busy else 0
        gpus.append({
            "index": index, "uuid": "GPU-mock-%04d" % index,
            "name": "NVIDIA H200", "util": util,
            "mem_used_mib": mem_used, "mem_total_mib": mem_total,
            "temp_c": 30 + (28 if busy else 0),
            "power_w": 78.0 + (430.0 if busy else 0.0),
            "power_limit_w": 700.0,
        })
        if not busy:
            continue
        owner = _MOCK_OWNERS[index % len(_MOCK_OWNERS)]
        pod = "%s-mock-train-%d-abcde" % (owner, index)
        for lane in range(3):  # several procs per GPU so scrolling matters
            pid += 1
            procs.append({
                "pid": pid, "gpu_index": index,
                "gpu_uuid": "GPU-mock-%04d" % index,
                "mem_mib": mem_used // 3,
                "owner": owner, "pod": pod, "pod_uid": None,
                "cmd": "python train.py --config exp%d/lane%d.yaml "
                       "--and-a-very-long-flag-to-test-clipping" % (index, lane),
                "started_utc": _iso_utc(now - 3600 * (index + 1)),
                "sm_util": max(0, util - 4 * lane),
                "attribution": "environ",
            })
    seen_pods = {}
    for proc in procs:  # pods rows must match proc pod names so ACT is real
        if proc["pod"] and proc["pod"] not in seen_pods:
            seen_pods[proc["pod"]] = proc
            pods.append({
                "owner": proc["owner"], "pod": proc["pod"],
                "node": NODE_NAME or "mock-node", "phase": "Running",
                "gpu": 1, "age": "%dh12m" % (proc["gpu_index"] + 1),
                "uid": "mock-uid-%d" % proc["gpu_index"],
                "start_iso": proc["started_utc"],
            })
    pods.append({  # allocated-but-idle row to exercise UI + stats
        "owner": "idleguy", "pod": "idleguy-holds-gpus-zzzzz",
        "node": NODE_NAME or "mock-node", "phase": "Running",
        "gpu": 2, "age": "6h1m", "uid": "mock-uid-idle",
        "start_iso": _iso_utc(now - 6 * 3600),
    })
    procs.append({  # unattributed proc to exercise the '?' path
        "pid": 99999, "gpu_index": 6, "gpu_uuid": "GPU-mock-0006",
        "mem_mib": 512, "owner": None, "pod": None, "pod_uid": None,
        "cmd": "/usr/bin/mystery --binary", "started_utc": _iso_utc(now - 120),
        "sm_util": None, "attribution": "none",
    })
    return {
        "driver": "580.126.16-mock", "source": "mock",
        "gpus": gpus, "procs": procs,
        "pods": {"ok": True, "source": "mock", "error": None, "rows": pods},
    }


# --- shared storage ----------------------------------------------------------


STORAGE_LABEL = os.environ.get("SGPU_STORAGE_LABEL", "pv-01/pv-02")
STORAGE_PATH = os.environ.get("SGPU_DATA_DIR", "/var/lib/sgpu")


def _storage_info():
    """Usage of the filesystem backing the shared lab volume. The stats dir
    is a subPath on pv-01, and statvfs reports the whole underlying array
    (pv-01 and pv-02 share one disk), so this is the lab-wide view."""
    if MOCK:
        total = 42.0 * 1024 ** 4
        used = 35.0 * 1024 ** 4
        return {"label": STORAGE_LABEL, "total_bytes": int(total),
                "used_bytes": int(used), "free_bytes": int(total - used),
                "pct": round(100.0 * used / total, 1)}
    try:
        st = os.statvfs(STORAGE_PATH)
    except (OSError, AttributeError):  # missing mount, or Windows dev box
        return None
    total = st.f_blocks * st.f_frsize
    free = st.f_bavail * st.f_frsize
    if total <= 0:
        return None
    used = total - free
    return {"label": STORAGE_LABEL, "total_bytes": total,
            "used_bytes": used, "free_bytes": free,
            "pct": round(100.0 * used / total, 1)}


# --- public API --------------------------------------------------------------


def nvml_status():
    return {"available": _pynvml is not None, "error": _nvml_error}


def collect(include_pods=True, force=False):
    """Build (or return cached) schema-2 snapshot."""
    with _lock:
        now = time.time()
        if not force and _cache["data"] is not None \
                and now - _cache["at"] < CACHE_TTL:
            return _cache["data"]

        if MOCK:
            base = _mock_snapshot(now)
            pods = base["pods"]
            procs = base["procs"]
            gpus = base["gpus"]
        else:
            base = _collect_nvml() or _collect_smi()
            pods = {"ok": False, "source": None, "error": "disabled",
                    "rows": []}
            uid_to_pod = {}
            if include_pods:
                import kube  # deferred: kube imports owner_from_name from here
                pods = kube.get_pods()
                uid_to_pod = pods.pop("uid_to_pod", {})
            procs = []
            live = set()
            for raw in base["procs"]:
                live.add(raw["pid"])
                attr = _attribute(raw["pid"], uid_to_pod)
                procs.append({
                    "pid": raw["pid"],
                    "gpu_index": raw["gpu_index"],
                    "gpu_uuid": raw["gpu_uuid"],
                    "mem_mib": raw["mem_mib"],
                    "owner": attr["owner"],
                    "pod": attr["pod"],
                    "pod_uid": attr["pod_uid"],
                    "cmd": attr["cmd"],
                    "started_utc": attr["started_utc"],
                    "sm_util": _sm_by_pid.get(raw["pid"]),
                    "attribution": attr["attribution"],
                })
            _evict_attr_cache(live)
            for pid in [p for p in _sm_by_pid if p not in live]:
                del _sm_by_pid[pid]
            gpus = base["gpus"]

        # Derived, render-friendly fields.
        owners_by_gpu = {}
        active_by_pod = {}
        for proc in procs:
            if proc["gpu_index"] is None:
                continue
            if proc["owner"]:
                owners_by_gpu.setdefault(proc["gpu_index"], set()).add(
                    proc["owner"])
            if proc["pod"]:
                active_by_pod.setdefault(proc["pod"], set()).add(
                    proc["gpu_index"])
        for gpu in gpus:
            gpu["owners"] = sorted(owners_by_gpu.get(gpu["index"], ()))
        for row in pods.get("rows", []):
            row["active"] = len(active_by_pod.get(row["pod"], ()))

        procs.sort(key=lambda p: (p["gpu_index"] if p["gpu_index"] is not None
                                  else 999, -(p["mem_mib"] or 0)))
        snapshot = {
            "schema": SCHEMA,
            "source": base["source"],
            "time_utc": _iso_utc(now),
            "node": NODE_NAME or "?",
            "driver": base["driver"],
            "sgpu_version": SGPU_VERSION,
            "gpus": gpus,
            "procs": procs,
            "pods": pods,
            "storage": _storage_info(),
        }
        if base.get("error"):
            snapshot["error"] = base["error"]
        _cache["at"] = now
        _cache["data"] = snapshot
        return snapshot
