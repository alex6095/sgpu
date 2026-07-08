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
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")


# --- update check -----------------------------------------------------------
# The server (monitor image) is upgraded centrally; the pip client is not.
# When the client falls behind the server, nudge the user to upgrade. The
# server version is cached for 6h so we don't add a round trip per command.

UPDATE_CHECK_TTL = 6 * 3600
_CACHE_DIR = os.path.join(os.path.expanduser("~"), ".cache", "sgpu")


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
    msg = ("sgpu %s is available (you have %s) — upgrade: pip install -U sgpu"
           % (server, __version__))
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
_RECONNECT_TRIES = 5
_POD_WAIT_SECONDS = 60


def _pod_running(namespace, pod):
    result = subprocess.run(
        ["kubectl", "get", "pod", pod, "-n", namespace,
         "-o", "jsonpath={.status.phase}"],
        capture_output=True, text=True)
    return result.returncode == 0 and result.stdout.strip() == "Running"


def _interactive(namespace, pod, pod_command, no_color):
    if not (sys.stdout.isatty() and sys.stdin.isatty()):
        _print(_fetch(namespace, pod, "/table?color=0&" + _cols_param()))
        return 0
    attempts = 0
    while True:
        try:
            code = subprocess.call(
                ["kubectl", "exec", "-it", "-n", namespace, pod, "--"]
                + pod_command)
        except KeyboardInterrupt:
            code = 130
        if code == 0:
            return 0
        # Abnormal end (137 = the in-pod process was SIGKILLed, e.g. the
        # monitor pod was recreated for an update). First un-wedge the
        # local terminal, then try to come back once the pod is Ready.
        sys.stdout.write(_TERM_RESTORE)
        sys.stdout.flush()
        if code == 130:
            return code
        attempts += 1
        if attempts > _RECONNECT_TRIES:
            print("sgpu: connection lost %d times; giving up (exit %d)"
                  % (attempts - 1, code), file=sys.stderr)
            return code
        print("sgpu: session ended unexpectedly (exit %d) — the monitor pod "
              "probably restarted for an update. Reconnecting..."
              % code, file=sys.stderr)
        waited = 0
        while waited < _POD_WAIT_SECONDS:
            if _pod_running(namespace, pod):
                break
            time.sleep(2)
            waited += 2
        else:
            _hint_on_failure(namespace, pod, "pod did not return to Running")
            return code
        time.sleep(1)  # let the server inside come up


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
    parser.add_argument("-V", "--version", action="version",
                        version="sgpu " + __version__)
    args = parser.parse_args(argv)

    if shutil.which("kubectl") is None:
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
        namespace = _resolve_namespace(args.namespace)
        if args.command == "watch":
            return _watch(namespace, args.pod,
                          args.number or args.refresh, args.no_color)
        # Hand the client version to the in-pod TUI so it can show an
        # upgrade banner when this client is behind the server.
        pod_command = (["nvitop"] if args.command == "nvitop"
                       else ["env", "SGPU_CLIENT_VERSION=" + __version__,
                             "python3", "/opt/gpu-monitor/tui.py",
                             str(args.refresh)])
        return _interactive(namespace, args.pod, pod_command, args.no_color)

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
