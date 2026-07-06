"""sgpu command-line client.

A thin cross-platform wrapper: all rendering happens inside the monitor pod,
this client only shells out to kubectl (fetch server-rendered text, or hand
the terminal to the in-pod TUI via `kubectl exec -it`).
"""

import argparse
import os
import shutil
import subprocess
import sys
import time

from sgpu import __version__

DEFAULT_NAMESPACE = os.environ.get("SGPU_NAMESPACE", "p-sgvr-node-02")
DEFAULT_POD = os.environ.get("SGPU_POD", "sangmin-gpu-monitor")
DEPLOY_URL = ("https://raw.githubusercontent.com/alex6095/sgpu/main/"
              "k8s/gpu-monitor.yaml")

COMMANDS = ("top", "once", "watch", "apps", "stats", "nvitop", "pods",
            "smi", "gpustat", "json", "health", "version")

EPILOG = """commands:
  (none) | top     interactive TUI (scroll, sort, owner filter)
  once             one-shot dashboard
  watch [sec]      simple refresh loop (for dumb terminals)
  apps             GPU process table with pod owners
  stats [days]     per-owner usage report (default 7 days)
  nvitop           raw nvitop TUI
  pods | smi | gpustat | json | health | version
"""


def _enable_ansi():
    if os.name == "nt":
        os.system("")  # nudges conhost/Windows Terminal into VT mode
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        pass


def _hint_on_failure(args, stderr_text):
    print("sgpu: cannot reach monitor pod '%s' in namespace '%s'"
          % (args.pod, args.namespace), file=sys.stderr)
    tail = [line for line in stderr_text.strip().splitlines()][-3:]
    for line in tail:
        print(line, file=sys.stderr)
    lowered = stderr_text.lower()
    if "notfound" in lowered or "not found" in lowered:
        print("hint: the monitor pod is not running. Deploy it with:",
              file=sys.stderr)
        print("  kubectl apply -f " + DEPLOY_URL, file=sys.stderr)
    elif "unauthorized" in lowered or "forbidden" in lowered \
            or "credentials" in lowered:
        print("hint: check your access: kubectl auth can-i get pods -n %s"
              % args.namespace, file=sys.stderr)


def _fetch(args, path):
    result = subprocess.run(
        ["kubectl", "exec", "-n", args.namespace, args.pod, "--",
         "curl", "-fsS", "http://127.0.0.1:8080" + path],
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        _hint_on_failure(args, result.stderr or result.stdout)
        return None
    return result.stdout


def _color_param(args):
    use = (not args.no_color) and sys.stdout.isatty()
    return "color=1" if use else "color=0"


def _cols_param():
    cols = shutil.get_terminal_size(fallback=(120, 40)).columns
    return "cols=%d" % max(40, cols)


def _print(text):
    if text is not None:
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")


def _interactive(args, pod_command):
    if not (sys.stdout.isatty() and sys.stdin.isatty()):
        _print(_fetch(args, "/table?color=0&" + _cols_param()))
        return 0
    return subprocess.call(
        ["kubectl", "exec", "-it", "-n", args.namespace, args.pod, "--"]
        + pod_command)


def _watch(args):
    seconds = args.number if args.number else args.refresh
    esc = "\x1b"
    sys.stdout.write(esc + "[?1049h" + esc + "[?25l")
    try:
        while True:
            frame = _fetch(args, "/table?%s&%s"
                           % (_color_param(args), _cols_param()))
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
    parser.add_argument("-n", "--namespace", default=DEFAULT_NAMESPACE)
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

    _enable_ansi()
    query = "%s&%s" % (_color_param(args), _cols_param())

    if args.command == "top":
        return _interactive(args, ["python3", "/opt/gpu-monitor/tui.py",
                                   str(args.refresh)])
    if args.command == "nvitop":
        return _interactive(args, ["nvitop"])
    if args.command == "watch":
        return _watch(args)

    paths = {
        "once": "/table?" + query,
        "apps": "/apps?" + query,
        "stats": "/stats?days=%d&%s" % (args.number or 7, query),
        "pods": "/pods",
        "smi": "/smi",
        "gpustat": "/gpustat",
        "json": "/json",
        "health": "/health",
        "version": "/version",
    }
    text = _fetch(args, paths[args.command])
    if text is None:
        return 1
    _print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
