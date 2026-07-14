"""sgpu command-line client.

A thin cross-platform wrapper: all rendering happens inside the monitor pod,
this client only shells out to kubectl (fetch server-rendered text, or hand
the terminal to the in-pod TUI via `kubectl exec -it`).

The MLXP cluster has one monitor pod per node-namespace (p-sgvr-node-01,
p-sgvr-node-02). Pick one with `-n 1`/`-n 2` (or a full namespace), or survey
both with `--all`. With no `-n`/$SGPU_NAMESPACE, the current kubectl context's
namespace is used.
"""

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import time

from sgpu import __version__

FALLBACK_NAMESPACE = "p-sgvr-node-02"
NODES = ("p-sgvr-node-01", "p-sgvr-node-02")
DEFAULT_POD = os.environ.get("SGPU_POD", "sangmin-gpu-monitor")
DEPLOY_URL = ("https://raw.githubusercontent.com/alex6095/sgpu/main/"
              "k8s/gpu-monitor.yaml")

INTERACTIVE = ("top", "nvitop", "watch")
COMMANDS = ("top", "once", "watch", "apps", "stats", "nvitop", "pods",
            "smi", "gpustat", "json", "health", "version")

EPILOG = """commands:
  (none) | top     interactive TUI (scroll, sort, owner filter, t=stats)
  once             one-shot dashboard
  watch [sec]      simple refresh loop (for dumb terminals)
  apps             GPU process table with pod owners
  stats [days]     per-owner usage report (default 7 days)
  nvitop           raw nvitop TUI
  pods | smi | gpustat | json | health | version

node selection:
  -n 1 | -n 2      shorthand for p-sgvr-node-01 / -node-02
  -n <namespace>   any namespace
  --all            survey every node (text commands only)
  (default: the current kubectl context's namespace, else p-sgvr-node-02)
"""


def _expand_ns(value):
    """Expand node shorthands; pass any other namespace through unchanged."""
    if value is None:
        return None
    key = value.strip().lower()
    if key in ("1", "01", "node-1", "node-01"):
        return "p-sgvr-node-01"
    if key in ("2", "02", "node-2", "node-02"):
        return "p-sgvr-node-02"
    return value


def _context_ns():
    """The current kubectl context's namespace, or None. Called only when no
    -n/$SGPU_NAMESPACE was given, so the extra subprocess never hits the TUI
    hot path."""
    try:
        result = subprocess.run(
            ["kubectl", "config", "view", "--minify",
             "-o", "jsonpath={..namespace}"],
            capture_output=True, text=True, timeout=5)
        return result.stdout.strip() or None
    except Exception:
        return None


def _resolve_namespace(explicit):
    if explicit:
        return _expand_ns(explicit)
    env = os.environ.get("SGPU_NAMESPACE")
    if env:
        return _expand_ns(env)
    return _context_ns() or FALLBACK_NAMESPACE


def _enable_ansi():
    if os.name == "nt":
        os.system("")  # nudges conhost/Windows Terminal into VT mode
    try:
        if sys.stdout.isatty():
            sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass


def _hint_on_failure(namespace, pod, stderr_text):
    print("sgpu: cannot reach monitor pod '%s' in namespace '%s'"
          % (pod, namespace), file=sys.stderr)
    for line in stderr_text.strip().splitlines()[-3:]:
        print(line, file=sys.stderr)
    lowered = stderr_text.lower()
    if "notfound" in lowered or "not found" in lowered:
        print("hint: the monitor pod is not running. Deploy it with:",
              file=sys.stderr)
        print("  kubectl apply -n %s -f %s" % (namespace, DEPLOY_URL),
              file=sys.stderr)
    elif "unauthorized" in lowered or "forbidden" in lowered \
            or "credentials" in lowered:
        print("hint: check your access: kubectl auth can-i get pods -n %s"
              % namespace, file=sys.stderr)


def _fetch(namespace, pod, path, quiet=False):
    result = subprocess.run(
        ["kubectl", "exec", "-n", namespace, pod, "--",
         "curl", "-fsS", "http://127.0.0.1:8080" + path],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        if not quiet:
            _hint_on_failure(namespace, pod, result.stderr or result.stdout)
        return None
    return result.stdout


def _color_param(no_color):
    use = (not no_color) and sys.stdout.isatty()
    return "color=1" if use else "color=0"


def _cols_param():
    cols = shutil.get_terminal_size(fallback=(120, 40)).columns
    return "cols=%d" % max(40, cols)


def _print(text):
    if text is not None:
        if hasattr(sys.stdout, "buffer") and not sys.stdout.isatty():
            data = text.encode("utf-8")
            if not text.endswith("\n"):
                data += b"\n"
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
            return
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")


def _print_json(payload):
    _print(json.dumps(payload, ensure_ascii=False, indent=2))


def _loads_json(text):
    return json.loads((text or "").lstrip("\ufeff"))


def _node_label(namespace):
    ns = _expand_ns(namespace) or namespace or ""
    if ns.endswith("-01"):
        return "1"
    if ns.endswith("-02"):
        return "2"
    return ns or "?"


def _now_utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _agent_header(kind, namespace, ok=True, sgpu_version=None,
                  generated_utc=None):
    return {
        "agent_schema": 1,
        "kind": kind,
        "ok": bool(ok),
        "generated_utc": generated_utc or _now_utc(),
        "sgpu_version": sgpu_version or __version__,
        "node": {
            "namespace": namespace,
            "label": _node_label(namespace),
        },
    }


def _agent_process(proc):
    return {
        "pid": proc.get("pid"),
        "gpu_index": proc.get("gpu_index"),
        "gpu_uuid": proc.get("gpu_uuid"),
        "owner": proc.get("owner") or "?",
        "pod": proc.get("pod"),
        "pod_uid": proc.get("pod_uid"),
        "mem_mib": proc.get("mem_mib"),
        "sm_util": proc.get("sm_util"),
        "started_utc": proc.get("started_utc"),
        "cmd": proc.get("cmd") or "",
        "attribution": proc.get("attribution"),
    }


def _agent_pod(row):
    return {
        "owner": row.get("owner") or "?",
        "pod": row.get("pod"),
        "phase": row.get("phase"),
        "request": row.get("gpu"),
        "active": row.get("active", 0),
        "node": row.get("node"),
        "age": row.get("age"),
        "uid": row.get("uid"),
        "start_iso": row.get("start_iso"),
    }


def _agent_snapshot(snapshot, namespace, kind="snapshot"):
    payload = _agent_header(
        kind, namespace, ok=True,
        sgpu_version=snapshot.get("sgpu_version"),
        generated_utc=snapshot.get("time_utc"))
    payload["node"].update({
        "name": snapshot.get("node"),
    })
    payload.update({
        "source": snapshot.get("source"),
        "driver": snapshot.get("driver"),
        "raw_schema": snapshot.get("schema"),
        "gpus": snapshot.get("gpus") or [],
        "processes": [_agent_process(p)
                      for p in snapshot.get("procs") or []],
        "pods": [_agent_pod(p)
                 for p in ((snapshot.get("pods") or {}).get("rows") or [])],
        "pod_status": {
            "ok": bool((snapshot.get("pods") or {}).get("ok")),
            "source": (snapshot.get("pods") or {}).get("source"),
            "error": (snapshot.get("pods") or {}).get("error"),
        },
        "free": snapshot.get("gpu_free"),
        "storage": snapshot.get("storage"),
    })
    if snapshot.get("error"):
        payload["error"] = snapshot.get("error")
        payload["ok"] = False
    return payload


def _agent_stats(result, namespace, days, scope):
    payload = _agent_header(
        "stats", namespace, ok=True,
        generated_utc=result.get("generated_utc"))
    payload.update({
        "days": int(days),
        "scope": scope or result.get("scope") or "local",
        "stats": result,
    })
    if result.get("notes"):
        payload["notes"] = result.get("notes")
    return payload


def _agent_health(text, namespace):
    text = (text or "").strip()
    parts = dict(part.split("=", 1) for part in text.split()
                 if "=" in part)
    payload = _agent_header("health", namespace,
                            ok=text.startswith("ok "))
    payload.update({
        "status": text.split()[0] if text else "unknown",
        "message": text,
        "gpus": int(parts["gpus"]) if parts.get("gpus", "").isdigit()
        else None,
        "source": parts.get("source"),
    })
    return payload


def _agent_version(version, namespace):
    payload = _agent_header(
        "version", namespace, ok=True,
        sgpu_version=version.get("sgpu_version"))
    payload["version"] = version
    return payload


# --- update check -----------------------------------------------------------
# The server (monitor image) is upgraded centrally; the pip client is not.
# When the client falls behind the server, nudge the user to upgrade. The
# server version is cached for 6h so we don't add a round trip per command.

UPDATE_CHECK_TTL = 6 * 3600
_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "sgpu")


def _upgrade_command():
    """The upgrade command that matches how this client was installed, so the
    nudge never tells a uv/pipx user to run a pip command they don't have."""
    hay = (sys.prefix + " " + (sys.argv[0] or "")).replace("\\", "/").lower()
    if "uv/tools" in hay or "/uv/" in hay:
        return "uv tool upgrade sgpu"
    if "pipx" in hay:
        return "pipx upgrade sgpu"
    return "pip install -U sgpu"


def _version_tuple(text):
    parts = []
    for chunk in str(text).split("."):
        digits = ""
        for ch in chunk:
            if ch.isdigit():
                digits += ch
            else:
                break
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def _is_outdated(client, server):
    if not server or not client:
        return False
    try:
        return _version_tuple(client) < _version_tuple(server)
    except Exception:
        return False


def _read_cached_server_version():
    try:
        with open(os.path.join(_CACHE_DIR, "update.json")) as fh:
            data = json.load(fh)
        return data.get("server_version"), data.get("checked_at", 0)
    except Exception:
        return None, 0


def _write_cached_server_version(server_version):
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(os.path.join(_CACHE_DIR, "update.json"), "w") as fh:
            json.dump({"server_version": server_version,
                       "checked_at": time.time()}, fh)
    except Exception:
        pass


def _server_version(namespace, pod):
    """Cached server version; refreshes via /version when stale (after the
    command's own output, so it never delays what the user asked for)."""
    server, checked_at = _read_cached_server_version()
    if server is not None and (time.time() - checked_at) <= UPDATE_CHECK_TTL:
        return server
    out = _fetch(namespace, pod, "/version", quiet=True)
    if out:
        try:
            fresh = json.loads(out).get("sgpu_version")
        except Exception:
            fresh = None
        if fresh:
            _write_cached_server_version(fresh)
            return fresh
    return server


def _emit_update_notice(namespace, pod, no_color):
    if os.environ.get("SGPU_NO_UPDATE_CHECK") == "1" \
            or not sys.stderr.isatty():
        return
    server = _server_version(namespace, pod)
    if not _is_outdated(__version__, server):
        return
    msg = ("sgpu %s is available (you have %s) — upgrade: %s"
           % (server, __version__, _upgrade_command()))
    if no_color:
        print("\n^ " + msg, file=sys.stderr)
    else:
        print("\n\x1b[1;33m↑ %s\x1b[0m" % msg, file=sys.stderr)


# Restore sequences for when the remote TUI dies without cleanup (pod
# restart during an update, LB timeout -> SIGKILL): disable mouse
# reporting, show the cursor, leave the alternate screen. Without this the
# shell is left spewing mouse escape codes.
_TERM_RESTORE = ("\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l"
                 "\x1b[?25h\x1b[?1049l")
_MAX_CONSECUTIVE_RECONNECTS = 5
# Five rapid reconnects trigger a capped recovery mode, rather than a process
# lifetime limit. The recovery window bounds an actual control-plane outage.
_STABLE_SESSION_SECONDS = 30
# ``tui.py`` deliberately exits 75 after 45s of unread output.  Treating that
# timeout as a normal 30s stable session would extend recovery forever, while
# treating every 75 forever as unstable would strand a genuinely long-running
# recovered session.  This threshold separates the default stall timeout from
# a session that demonstrably delivered output for a meaningful interval.
_WATCHDOG_STABLE_SESSION_SECONDS = 90
_RECONNECT_RECOVERY_WINDOW_SECONDS = 300
_POD_STATUS_TIMEOUT_SECONDS = 5
_HEALTH_TIMEOUT_SECONDS = 8
_SIGNAL_SETTLE_SECONDS = 5
_SIGNAL_SETTLE_INITIAL_DELAY_SECONDS = 0.25
_RECONNECT_INITIAL_DELAY_SECONDS = 1
_RECONNECT_MAX_DELAY_SECONDS = 16
_RECONNECT_JITTER_FRACTION = 0.20

# These errors cannot be repaired by reconnecting. Check them before pod
# state because an RBAC failure also prevents ``kubectl get pod`` from
# observing that state.
_PERMANENT_EXEC_ERROR_MARKERS = (
    "forbidden",
    "unauthorized",
    "permission denied",
    "you must be logged in",
    "provide credentials",
    "executable file not found",
    "no such file or directory",
    "unknown command",
)
_TRANSPORT_ERROR_MARKERS = (
    "unexpected eof",
    "connection reset",
    "connection closed",
    "stream error",
    "http2",
    "websocket",
    "spdy",
    "tls handshake timeout",
    "i/o timeout",
    "context deadline exceeded",
)


def _pod_state(namespace, pod):
    """Return the monitor lifecycle state without raising on a bad cluster.

    A phase of ``Running`` is not enough to safely reconnect: Kubernetes can
    report it while the container is terminating, restarting, or not Ready.
    The UID catches a replacement pod during a rollout and restart_count
    catches a monitor-container restart in the same pod.
    """
    state = {
        "uid": None,
        "phase": None,
        "ready": False,
        "restart_count": 0,
        "monitor_image": None,
        "container_id": None,
        "started_at": None,
        "error": None,
    }
    try:
        result = subprocess.run(
            ["kubectl", "get", "pod", pod, "-n", namespace, "-o", "json"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=_POD_STATUS_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired) as exc:
        state["error"] = "%s: %s" % (type(exc).__name__, exc)
        return state
    if result.returncode != 0:
        state["error"] = (result.stderr or result.stdout or
                          "kubectl get pod exited %d" % result.returncode).strip()
        return state
    try:
        payload = json.loads(result.stdout)
    except (TypeError, ValueError) as exc:
        state["error"] = "invalid kubectl pod JSON: %s" % exc
        return state

    metadata = payload.get("metadata") or {}
    spec = payload.get("spec") or {}
    status = payload.get("status") or {}
    state["uid"] = metadata.get("uid")
    state["phase"] = status.get("phase")
    for condition in status.get("conditions") or []:
        if condition.get("type") == "Ready":
            state["ready"] = (
                state["phase"] == "Running"
                and condition.get("status") == "True"
                and not metadata.get("deletionTimestamp"))
            break
    spec_containers = spec.get("containers") or []
    status_containers = status.get("containerStatuses") or []
    monitor_spec = next(
        (container for container in spec_containers
         if container.get("name") == "monitor"),
        spec_containers[0] if spec_containers else {})
    monitor_status = next(
        (container for container in status_containers
         if container.get("name") == "monitor"),
        status_containers[0] if status_containers else {})
    state["monitor_image"] = monitor_spec.get("image")
    state["container_id"] = monitor_status.get("containerID")
    running = (monitor_status.get("state") or {}).get("running") or {}
    state["started_at"] = running.get("startedAt")
    try:
        state["restart_count"] = int(monitor_status.get("restartCount", 0))
    except (TypeError, ValueError):
        pass
    return state


def _pod_running(namespace, pod):
    """Compatibility helper for callers/tests that only need readiness."""
    return _pod_state(namespace, pod)["ready"]


def _pod_restarted_or_recreated(before, after):
    return (_pod_desired_image_changed(before, after)
            or _pod_instance_changed(before, after))


def _pod_desired_image_changed(before, after):
    """Whether the pod spec now asks for a different monitor image."""
    return bool(before and after and before.get("monitor_image")
                and after.get("monitor_image")
                and before["monitor_image"] != after["monitor_image"])


def _pod_instance_changed(before, after):
    """Whether Kubernetes has actually replaced/restarted the monitor."""
    if not before or not after:
        return False
    if (before.get("uid") and after.get("uid")
            and before["uid"] != after["uid"]):
        return True
    if after.get("restart_count", 0) > before.get("restart_count", 0):
        return True
    for field in ("container_id", "started_at"):
        if (before.get(field) and after.get(field)
                and before[field] != after[field]):
            return True
    return False


def _pod_not_found(state):
    if not state:
        return False
    text = (state.get("error") or "").lower()
    return "notfound" in text or "not found" in text


def _is_interrupt_exit(code):
    # POSIX uses -SIGINT for a signalled child; Windows can expose the raw
    # STATUS_CONTROL_C_EXIT value. KeyboardInterrupt is handled separately.
    return code in (130, -2, -1073741510, 3221225786)


def _is_permanent_exec_failure(stderr_text):
    lowered = (stderr_text or "").lower()
    return any(marker in lowered for marker in _PERMANENT_EXEC_ERROR_MARKERS)


def _failure_diagnostics(stderr_text, pod_state):
    return "\n".join(part for part in (
        stderr_text, (pod_state or {}).get("error")) if part)


def _is_transport_failure(stderr_text):
    lowered = (stderr_text or "").lower()
    return any(marker in lowered for marker in _TRANSPORT_ERROR_MARKERS) \
        or not lowered.strip()


def _is_remote_signal_termination(code, stderr_text):
    """Whether kubectl reports that the remote process received SIGKILL/TERM."""
    lowered = (stderr_text or "").lower()
    return (code in (137, 143)
            or "command terminated with exit code 137" in lowered
            or "command terminated with exit code 143" in lowered)


def _is_container_exec_race(stderr_text):
    """The pod exists, but its named container is between exec endpoints."""
    return ("unable to upgrade connection: container not found (\"monitor\")"
            in (stderr_text or "").lower())


def _is_retryable_exec_failure(before, after, stderr_text,
                               tui_watchdog_exit=False,
                               remote_signal_termination=False):
    """Separate pod/transport loss from an in-pod command failure."""
    diagnostics = _failure_diagnostics(stderr_text, after)
    if _is_permanent_exec_failure(diagnostics):
        return False
    # A disappearance after a known pod can be an update rollout. A pod that
    # was absent before the session cannot be repaired by reconnecting.
    if _pod_not_found(after) and not (before or {}).get("uid"):
        return False
    if not after.get("ready") or _pod_restarted_or_recreated(before, after):
        return True
    if _is_container_exec_race(diagnostics):
        return True
    if tui_watchdog_exit and (
            "command terminated with exit code 75"
            in diagnostics.lower()):
        return True
    # A signal is lifecycle evidence only when the pod actually changed.  A
    # bounded settling re-probe is performed by _interactive before this
    # function is called; if that still sees the same Ready container, do not
    # disguise a remote application exit as a rollout.
    if remote_signal_termination:
        return False
    # kubectl reports a non-zero exit from tui.py this way. If the exact same
    # monitor is still Ready, reconnecting just starts the same crashing command
    # five times and hides its useful diagnostic.
    if "command terminated with exit code" in diagnostics.lower():
        return False
    return _is_transport_failure(diagnostics)


def _is_tui_watchdog_exit(code, stderr_text, enabled=False):
    """True only for tui.py's deliberate unread-output watchdog exit."""
    return (enabled and code == 75
            and "command terminated with exit code 75"
            in (stderr_text or "").lower())


def _interactive_exec(command):
    """Run kubectl with the TUI attached and retain its terminal diagnostic.

    stdout/stdin deliberately inherit the terminal, preserving ``kubectl exec
    -it``. Capturing only stderr lets the caller classify an EOF/RBAC/remote
    command exit while subprocess.run() still drains the pipe and reaps the
    child on every path.
    """
    try:
        result = subprocess.run(
            command, stderr=subprocess.PIPE, text=True, encoding="utf-8",
            errors="replace")
    except KeyboardInterrupt:
        return 130, ""
    stderr_text = result.stderr or ""
    if stderr_text:
        sys.stderr.write(stderr_text)
        sys.stderr.flush()
    return result.returncode, stderr_text


def _monitor_healthy(namespace, pod):
    """Whether the in-pod server has bound /health after a reconnection."""
    try:
        result = subprocess.run(
            ["kubectl", "exec", "-n", namespace, pod, "--", "curl", "-sS",
             "--max-time", "3", "-o", "/dev/null", "-w", "%{http_code}",
             "http://127.0.0.1:8080/health"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=_HEALTH_TIMEOUT_SECONDS)
    except (OSError, subprocess.TimeoutExpired):
        return False
    # 503 is a *degraded GPU data source*, not an unavailable HTTP server. The
    # TUI can still display its diagnostic, so it is safe to attach to it.
    return result.returncode == 0 and result.stdout.strip() in ("200", "503")


def _wait_for_ready_pod(namespace, pod, require_health=False,
                        timeout=_RECONNECT_RECOVERY_WINDOW_SECONDS,
                        baseline=None, require_instance_change=False):
    """Wait for a Ready/healthy monitor, optionally after an instance switch."""
    deadline = time.monotonic() + timeout
    delay = _RECONNECT_INITIAL_DELAY_SECONDS
    while True:
        state = _pod_state(namespace, pod)
        now = time.monotonic()
        if (require_instance_change
                and not _pod_instance_changed(baseline, state)):
            ready_for_exec = False
        else:
            ready_for_exec = state["ready"]
        if ready_for_exec and not require_health:
            return state
        if ready_for_exec and _monitor_healthy(namespace, pod):
            return state
        remaining = deadline - now
        if remaining <= 0:
            return None
        time.sleep(min(delay, remaining))
        delay = min(_RECONNECT_MAX_DELAY_SECONDS, delay * 2)


def _settle_signal_termination(namespace, pod, before, after):
    """Re-probe a briefly stale Ready status after remote SIGKILL/SIGTERM.

    A pod image patch changes spec.image before kubelet updates
    containerStatuses. A just-ended exec can therefore observe the old Ready
    container once. Keep this probe short and use only concrete lifecycle
    changes; a signal with an otherwise identical pod remains fail-fast.
    """
    state = after
    deadline = time.monotonic() + _SIGNAL_SETTLE_SECONDS
    delay = _SIGNAL_SETTLE_INITIAL_DELAY_SECONDS
    while not _pod_restarted_or_recreated(before, state):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return state
        time.sleep(min(delay, remaining))
        state = _pod_state(namespace, pod)
        delay = min(1.0, delay * 2)
    return state


def _reconnect_delay(consecutive_failures, random_value=None):
    """Bounded exponential backoff with small jitter for concurrent clients."""
    exponent = min(20, max(0, consecutive_failures - 1))
    base = min(_RECONNECT_MAX_DELAY_SECONDS,
               _RECONNECT_INITIAL_DELAY_SECONDS * (2 ** exponent))
    if random_value is None:
        random_value = random.random()
    jitter = 1 + _RECONNECT_JITTER_FRACTION * (2 * random_value - 1)
    return min(_RECONNECT_MAX_DELAY_SECONDS, base * jitter)


def _interactive(namespace, pod, pod_command, no_color):
    if not (sys.stdout.isatty() and sys.stdin.isatty()):
        _print(_fetch(namespace, pod, "/table?color=0&" + _cols_param()))
        return 0
    consecutive_failures = 0
    try:
        pod_before = _pod_state(namespace, pod)
    except KeyboardInterrupt:
        return 130
    # Do not launch an interactive child when the lifecycle probe has already
    # established a permanent problem.  Besides producing a clearer error,
    # this is important for least-privileged kubeconfigs: a denied ``get`` or
    # a known-missing pod must not be followed by a needless ``pods/exec``.
    initial_diagnostics = _failure_diagnostics("", pod_before)
    if (_pod_not_found(pod_before)
            or _is_permanent_exec_failure(initial_diagnostics)):
        _hint_on_failure(namespace, pod, initial_diagnostics)
        return 1
    require_health = "tui.py" in pod_command
    recovery_started_at = None
    while True:
        started_at = time.monotonic()
        code, stderr_text = _interactive_exec(
            ["kubectl", "exec", "-it", "-n", namespace, pod, "--"]
            + pod_command)
        if code == 0:
            return 0
        sys.stdout.write(_TERM_RESTORE)
        sys.stdout.flush()
        if _is_interrupt_exit(code):
            return code

        try:
            pod_after = _pod_state(namespace, pod)
        except KeyboardInterrupt:
            return 130
        remote_signal_termination = _is_remote_signal_termination(
            code, stderr_text)
        post_diagnostics = _failure_diagnostics(stderr_text, pod_after)
        if (remote_signal_termination and pod_after.get("ready")
                and not _is_permanent_exec_failure(post_diagnostics)
                and not _pod_restarted_or_recreated(pod_before, pod_after)):
            try:
                pod_after = _settle_signal_termination(
                    namespace, pod, pod_before, pod_after)
            except KeyboardInterrupt:
                return 130
        if not _is_retryable_exec_failure(
                pod_before, pod_after, stderr_text,
                tui_watchdog_exit=require_health,
                remote_signal_termination=remote_signal_termination):
            diagnostics = _failure_diagnostics(stderr_text, pod_after)
            if (_pod_not_found(pod_after)
                    or _is_permanent_exec_failure(diagnostics)):
                _hint_on_failure(namespace, pod, diagnostics)
            elif remote_signal_termination:
                print("sgpu: remote TUI received SIGKILL/SIGTERM but the "
                      "monitor lifecycle stayed unchanged after settling; "
                      "not reconnecting", file=sys.stderr)
            else:
                print("sgpu: monitor session failed without a recoverable pod "
                      "or transport interruption (exit %d); not reconnecting"
                      % code, file=sys.stderr)
            return code

        # The budget covers only a burst of failures. A session that stayed
        # healthy for this long proved that the transport works, so an update
        # hours later starts a fresh budget instead of inheriting old blips.
        # A remote-output watchdog is different: its 45s no-progress timeout
        # necessarily exceeds the normal stable threshold.  Repeated default
        # watchdog exits retain the original five-minute deadline, but a
        # genuinely long recovered session gets a fresh budget even if it
        # eventually ends through the watchdog path.
        ended_at = time.monotonic()
        watchdog_exit = _is_tui_watchdog_exit(
            code, stderr_text, enabled=require_health)
        stable_threshold = (_WATCHDOG_STABLE_SESSION_SECONDS
                            if watchdog_exit else _STABLE_SESSION_SECONDS)
        if ended_at - started_at >= stable_threshold:
            consecutive_failures = 0
            recovery_started_at = None
        if recovery_started_at is None:
            recovery_started_at = ended_at
        recovery_deadline = (
            recovery_started_at + _RECONNECT_RECOVERY_WINDOW_SECONDS)
        consecutive_failures += 1
        if (consecutive_failures > _MAX_CONSECUTIVE_RECONNECTS
                and ended_at >= recovery_deadline):
            print("sgpu: recovery window expired after %d unstable "
                  "connections; giving up (exit %d)"
                  % (consecutive_failures - 1, code), file=sys.stderr)
            return code

        rollout = _pod_restarted_or_recreated(pod_before, pod_after)
        require_instance_change = (
            _pod_desired_image_changed(pod_before, pod_after)
            and not _pod_instance_changed(pod_before, pod_after))
        try:
            ready_state = _wait_for_ready_pod(
                namespace, pod, require_health=require_health,
                timeout=max(0.0, recovery_deadline - time.monotonic()),
                baseline=pod_before,
                require_instance_change=require_instance_change)
        except KeyboardInterrupt:
            return 130
        if ready_state is None:
            _hint_on_failure(namespace, pod,
                             "pod was not Ready/healthy before reconnect")
            return code
        reason = "monitor pod changed" if rollout else "transport ended"
        delay = _reconnect_delay(consecutive_failures)
        if consecutive_failures <= _MAX_CONSECUTIVE_RECONNECTS:
            progress = "attempt %d/%d" % (
                consecutive_failures, _MAX_CONSECUTIVE_RECONNECTS)
        else:
            remaining = max(0, int(recovery_deadline - time.monotonic()))
            progress = "extended recovery; %ds left" % remaining
        print("sgpu: %s unexpectedly (exit %d); reconnecting in %ss "
              "(%s)..."
              % (reason, code, delay, progress), file=sys.stderr)
        try:
            time.sleep(delay)
        except KeyboardInterrupt:
            return 130
        pod_before = ready_state


def _watch(namespace, pod, seconds, no_color):
    esc = "\x1b"
    sys.stdout.write(esc + "[?1049h" + esc + "[?25l")
    try:
        while True:
            frame = _fetch(namespace, pod, "/table?%s&%s"
                           % (_color_param(no_color), _cols_param()))
            if frame is None:
                frame = "sgpu: fetch failed; retrying..."
            sys.stdout.write(esc + "[H" + frame + "\n" + esc + "[0J")
            sys.stdout.flush()
            time.sleep(max(1, seconds))
    except KeyboardInterrupt:
        return 0
    finally:
        sys.stdout.write(esc + "[?25h" + esc + "[?1049l")
        sys.stdout.flush()


def _text_path(command, number, no_color):
    query = "%s&%s" % (_color_param(no_color), _cols_param())
    return {
        "once": "/table?" + query,
        "apps": "/apps?" + query,
        "stats": "/stats?days=%d&%s" % (number or 7, query),
        "pods": "/pods",
        "smi": "/smi",
        "gpustat": "/gpustat",
        "json": "/json",
        "health": "/health",
        "version": "/version",
    }[command]


def _stats_path(number, no_color, scope=None, json_format=False):
    query = ["days=%d" % (number or 7)]
    if json_format:
        query.append("format=json")
    else:
        query.extend([_color_param(no_color), _cols_param()])
    if scope and scope != "local":
        query.append("scope=lab" if scope == "lab" else "scope=%s" % scope)
    return "/stats?" + "&".join(query)


def _json_scope(scope, namespace, all_nodes=False):
    if all_nodes:
        return "lab"
    if not scope:
        return "local"
    key = str(scope).strip().lower()
    if key in ("lab", "all"):
        return "lab"
    if key in ("local", "node", "this"):
        return "local"
    if key in ("1", "01", "node-1", "node-01"):
        return "1"
    if key in ("2", "02", "node-2", "node-02"):
        return "2"
    return key


def _agent_json_for(namespace, pod, command, number, scope=None, quiet=False):
    if command in ("once", "json", "apps", "pods"):
        text = _fetch(namespace, pod, "/json", quiet=quiet)
        if text is None:
            return None
        snapshot = _loads_json(text)
        kind = {"once": "snapshot", "json": "snapshot",
                "apps": "processes", "pods": "pods"}[command]
        payload = _agent_snapshot(snapshot, namespace, kind=kind)
        if command == "apps":
            payload.pop("pods", None)
            payload.pop("pod_status", None)
            payload.pop("free", None)
            payload.pop("storage", None)
        elif command == "pods":
            payload.pop("processes", None)
            payload.pop("gpus", None)
            payload.pop("free", None)
            payload.pop("storage", None)
        return payload
    if command == "stats":
        resolved_scope = _json_scope(scope, namespace)
        stats_scope = "lab" if resolved_scope == "lab" else None
        text = _fetch(namespace, pod, _stats_path(
            number, True, scope=stats_scope, json_format=True), quiet=quiet)
        if text is None:
            return None
        return _agent_stats(_loads_json(text), namespace,
                            number or 7, resolved_scope)
    if command == "health":
        text = _fetch(namespace, pod, "/health", quiet=quiet)
        if text is None:
            return None
        return _agent_health(text, namespace)
    if command == "version":
        text = _fetch(namespace, pod, "/version", quiet=quiet)
        if text is None:
            return None
        return _agent_version(_loads_json(text), namespace)
    text = _fetch(namespace, pod, _text_path(command, number, True),
                  quiet=quiet)
    if text is None:
        return None
    payload = _agent_header(command, namespace, ok=True)
    payload["text"] = text
    return payload


def _emit_agent_error(namespace, error):
    return {
        "agent_schema": 1,
        "kind": "node_result",
        "ok": False,
        "generated_utc": _now_utc(),
        "sgpu_version": __version__,
        "node": {
            "namespace": namespace,
            "label": _node_label(namespace),
        },
        "error": error,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="sgpu",
        description="Simple GPU monitor for the SGVR lab MLXP cluster.",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", nargs="?", default="top",
                        choices=COMMANDS, metavar="command")
    parser.add_argument("number", nargs="?", type=int, default=0,
                        metavar="N",
                        help="days for stats, seconds for watch")
    parser.add_argument("-n", "--namespace", default=None,
                        help="namespace or node shorthand (1, 2, node-01)")
    parser.add_argument("-a", "--all", action="store_true",
                        help="survey every node (text commands only)")
    parser.add_argument("--pod", default=DEFAULT_POD)
    parser.add_argument("-r", "--refresh", type=int, default=2,
                        help="TUI/watch refresh interval (default 2)")
    parser.add_argument("--no-color", action="store_true")
    parser.add_argument("--json", action="store_true",
                        help="emit stable agent_schema=1 JSON for text commands")
    parser.add_argument("--scope", default=None,
                        help="stats JSON scope: local, 1, 2, or lab")
    parser.add_argument("-V", "--version", action="version",
                        version="sgpu " + __version__)
    args = parser.parse_args(argv)

    if shutil.which("kubectl") is None:
        if args.json:
            payload = _agent_header(args.command, _resolve_namespace(
                args.namespace), ok=False)
            payload["error"] = "kubectl not found on PATH"
            _print_json(payload)
            return 127
        print("sgpu: kubectl not found on PATH", file=sys.stderr)
        print("hint: install kubectl and configure the MLXP kubeconfig "
              "(see the sgpu README)", file=sys.stderr)
        return 127

    if args.all and args.command in INTERACTIVE:
        print("sgpu: --all cannot be used with '%s' (it hands the terminal "
              "to one pod).\n      Pick a node, e.g. sgpu -n 1 %s"
              % (args.command, args.command), file=sys.stderr)
        return 2

    _enable_ansi()

    if args.command in INTERACTIVE:
        if args.json:
            print("sgpu: --json cannot be used with interactive '%s'"
                  % args.command, file=sys.stderr)
            return 2
        namespace = _resolve_namespace(args.namespace)
        if args.command == "watch":
            return _watch(namespace, args.pod,
                          args.number or args.refresh, args.no_color)
        # Hand the client version to the in-pod TUI so it can show an
        # upgrade banner when this client is behind the server.
        pod_command = (["nvitop"] if args.command == "nvitop"
                       else ["env", "SGPU_CLIENT_VERSION=" + __version__,
                             "SGPU_UPGRADE_CMD=" + _upgrade_command(),
                             "python3", "/opt/gpu-monitor/tui.py",
                             str(args.refresh)])
        return _interactive(namespace, args.pod, pod_command, args.no_color)

    if args.json:
        if args.all and args.command == "stats":
            preferred = _resolve_namespace(args.namespace)
            for ns in [preferred] + [n for n in NODES if n != preferred]:
                try:
                    payload = _agent_json_for(
                        ns, args.pod, "stats", args.number,
                        scope="lab", quiet=True)
                except Exception:
                    payload = None
                if payload is not None:
                    _print_json(payload)
                    return 0
            _print_json(_emit_agent_error("all",
                                          "no reachable monitor pod"))
            return 1
        if args.all:
            nodes = []
            succeeded = False
            for ns in NODES:
                try:
                    payload = _agent_json_for(ns, args.pod, args.command,
                                              args.number, args.scope,
                                              quiet=True)
                except Exception as exc:
                    payload = _emit_agent_error(
                        ns, "%s: %s" % (type(exc).__name__, exc))
                if payload is None:
                    nodes.append(_emit_agent_error(
                        ns, "monitor pod unreachable"))
                    continue
                nodes.append(payload)
                succeeded = True
            _print_json({
                "agent_schema": 1,
                "kind": args.command + "_all",
                "ok": succeeded,
                "generated_utc": _now_utc(),
                "sgpu_version": __version__,
                "nodes": nodes,
            })
            return 0 if succeeded else 1
        namespace = _resolve_namespace(args.namespace)
        if args.command == "stats":
            scope = _json_scope(args.scope, namespace)
            if scope in ("1", "2"):
                namespace = _expand_ns(scope)
        else:
            scope = args.scope
        try:
            payload = _agent_json_for(namespace, args.pod, args.command,
                                      args.number, scope, quiet=True)
        except Exception as exc:
            payload = _emit_agent_error(
                namespace, "%s: %s" % (type(exc).__name__, exc))
        if payload is None:
            _print_json(_emit_agent_error(namespace,
                                          "monitor pod unreachable"))
            return 1
        _print_json(payload)
        return 0 if payload.get("ok") else 1

    path = _text_path(args.command, args.number, args.no_color)

    if args.all and args.command == "stats":
        # One lab-wide merged report (any reachable monitor renders it by
        # pulling its peers' data server-side) — not two separate reports.
        preferred = _resolve_namespace(args.namespace)
        for ns in [preferred] + [n for n in NODES if n != preferred]:
            text = _fetch(ns, args.pod, path + "&scope=lab", quiet=True)
            if text is not None:
                _print(text)
                _emit_update_notice(ns, args.pod, args.no_color)
                return 0
        print("sgpu: no reachable monitor pod on any node", file=sys.stderr)
        return 1

    if args.all:
        succeeded = False
        used_ns = None
        for ns in NODES:
            print("\x1b[1;36m=== %s ===\x1b[0m" % ns
                  if sys.stdout.isatty() and not args.no_color
                  else "=== %s ===" % ns)
            text = _fetch(ns, args.pod, path, quiet=True)
            if text is None:
                print("  (no reachable monitor pod; skipped)\n")
                continue
            _print(text)
            print("")
            succeeded = True
            used_ns = ns
        if used_ns:
            _emit_update_notice(used_ns, args.pod, args.no_color)
        return 0 if succeeded else 1

    namespace = _resolve_namespace(args.namespace)
    text = _fetch(namespace, args.pod, path)
    if text is None:
        return 1
    _print(text)
    _emit_update_notice(namespace, args.pod, args.no_color)
    return 0


if __name__ == "__main__":
    sys.exit(main())
