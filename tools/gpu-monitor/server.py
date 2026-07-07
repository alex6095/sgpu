"""sgpu monitor HTTP server (pod entry point).

Thin layer only: routing and query parsing. Data collection lives in
collector.py, k8s access in kube.py, layout in render.py, usage accounting
in statsdb.py / statsagg.py. Endpoints render server-side so that every
client — including plain `kubectl exec ... curl` with nothing installed —
gets the same dashboard. Plain text by default; ANSI color is opt-in via
?color=1 because the server cannot see the client's TTY.
"""

import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

import collector
import render

try:
    import statsdb
    import statsagg
    STATS_IMPORT_ERROR = None
except Exception as exc:  # never let a stats bug take monitoring down
    statsdb = None
    statsagg = None
    STATS_IMPORT_ERROR = "%s: %s" % (type(exc).__name__, exc)


STATS_QUERY_CACHE_TTL = float(os.environ.get("SGPU_STATS_QUERY_CACHE_TTL", "10"))
_STATS_QUERY_CACHE = {}
_STATS_QUERY_LOCK = threading.Lock()

def run_cmd(cmd, timeout=8):
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True,
                              timeout=timeout)
        return proc.returncode == 0, proc.stdout + proc.stderr
    except Exception as exc:
        return False, "%s: %s" % (type(exc).__name__, exc)


def footer_notes():
    notes = []
    if statsdb is None:
        notes.append("stats disabled: %s" % STATS_IMPORT_ERROR)
    else:
        status = statsdb.status()
        if status.get("fallback"):
            notes.append("stats degraded: writing to %s (hostPath "
                         "unavailable; history is lost on pod restart)"
                         % status.get("data_dir"))
    return notes


def _stats_signature(data_dir):
    if statsagg is None:
        return ()
    return tuple((f.get("name"), f.get("size"), f.get("mtime_iso"))
                 for f in statsagg.list_files(data_dir))


def query_stats_cached(data_dir, days, owner):
    if STATS_QUERY_CACHE_TTL <= 0:
        return statsagg.query(data_dir, days=days, owner=owner)
    signature = _stats_signature(data_dir)
    key = (data_dir, days, owner, signature)
    now = time.time()
    with _STATS_QUERY_LOCK:
        entry = _STATS_QUERY_CACHE.get(key)
        if entry is not None and now - entry[0] < STATS_QUERY_CACHE_TTL:
            return entry[1]
    result = statsagg.query(data_dir, days=days, owner=owner)
    with _STATS_QUERY_LOCK:
        for old_key, (at, _value) in list(_STATS_QUERY_CACHE.items()):
            if now - at >= STATS_QUERY_CACHE_TTL:
                del _STATS_QUERY_CACHE[old_key]
        _STATS_QUERY_CACHE[key] = (now, result)
    return result


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_body(self, code, body, content_type="text/plain; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_json(self, code, payload):
        self.send_body(code, json.dumps(payload, indent=2, ensure_ascii=False),
                       "application/json")

    def send_lines(self, code, lines,
                   content_type="text/plain; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        for line in lines:
            data = line if isinstance(line, bytes) else line.encode("utf-8")
            self.wfile.write(data)
            self.wfile.write(b"\n")

    def do_GET(self):
        try:
            self.route()
        except BrokenPipeError:
            pass
        except Exception as exc:
            try:
                self.send_body(500, "sgpu server error: %s: %s\n"
                               % (type(exc).__name__, exc))
            except Exception:
                pass

    def route(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query)

        def q(name, default=None):
            return query.get(name, [default])[0]

        color = q("color") == "1"
        try:
            width = max(40, min(500, int(q("cols") or 120)))
        except ValueError:
            width = 120
        unicode_ok = q("ascii") != "1"

        if path in ("/", "/table", "/short"):
            snapshot = collector.collect()
            self.send_body(200, render.render_text(
                snapshot, width=width, color=color, unicode_ok=unicode_ok,
                footer_notes=footer_notes()))
            return
        if path == "/apps":
            self.send_body(200, render.render_procs_text(
                collector.collect(), width=width, color=color))
            return
        if path == "/json":
            self.send_json(200, collector.collect())
            return
        if path == "/pods":
            pods = collector.collect()["pods"]
            self.send_json(200 if pods.get("ok") else 503, pods)
            return
        if path == "/health":
            snapshot = collector.collect()
            healthy = bool(snapshot.get("gpus")) or snapshot["source"] == "mock"
            self.send_body(200 if healthy else 503,
                           "%s gpus=%d source=%s\n" % (
                               "ok" if healthy else "degraded",
                               len(snapshot.get("gpus", [])),
                               snapshot["source"]))
            return
        if path == "/smi":
            ok, out = run_cmd(["nvidia-smi"])
            self.send_body(200 if ok else 503, out)
            return
        if path == "/topo":
            ok, out = run_cmd(["nvidia-smi", "topo", "-m"])
            self.send_body(200 if ok else 503, out)
            return
        if path == "/gpustat":
            ok, out = run_cmd(["gpustat", "--no-color"], timeout=6)
            self.send_body(200 if ok else 503, out)
            return
        if path == "/version":
            payload = {
                "sgpu_version": collector.SGPU_VERSION,
                "schema": collector.SCHEMA,
                "source": collector.collect()["source"],
                "nvml": collector.nvml_status(),
                "stats": (statsdb.status() if statsdb
                          else {"error": STATS_IMPORT_ERROR}),
            }
            self.send_json(200, payload)
            return
        if path.startswith("/stats"):
            self.route_stats(path, q, color, width)
            return
        self.send_body(404, "not found\n")

    def route_stats(self, path, q, color, width):
        if statsdb is None or statsagg is None:
            self.send_body(503, "stats unavailable: %s\n" % STATS_IMPORT_ERROR)
            return
        data_dir = statsdb.resolved_data_dir()
        if path == "/stats/files":
            self.send_json(200, statsagg.list_files(data_dir))
            return
        if path == "/stats/raw":
            date = (q("date") or "").replace("-", "")
            if not date.isdigit() or len(date) != 8:
                self.send_body(400, "usage: /stats/raw?date=YYYYMMDD\n")
                return
            try:
                lines = statsagg.iter_raw_lines(data_dir, date)
            except FileNotFoundError:
                self.send_body(404, "no samples for %s\n" % date)
                return
            self.send_lines(200, lines, "application/x-ndjson; charset=utf-8")
            return
        if path == "/stats":
            try:
                days = max(1, min(365, int(q("days") or 7)))
            except ValueError:
                days = 7
            result = query_stats_cached(data_dir, days=days, owner=q("owner"))
            if q("format") == "json":
                self.send_json(200, result)
                return
            self.send_body(200, statsagg.render_stats_text(
                result, color=color, width=width,
                unicode_ok=(q("ascii") != "1")))
            return
        self.send_body(404, "not found\n")

    def log_message(self, fmt_string, *args):
        print("%s - %s" % (self.address_string(), fmt_string % args),
              flush=True)


if __name__ == "__main__":
    if statsdb is not None:
        try:
            statsdb.start_sampler(collector.collect)
        except Exception as exc:
            print("sgpu: stats sampler failed to start: %s" % exc, flush=True)
    else:
        print("sgpu: stats modules unavailable: %s" % STATS_IMPORT_ERROR,
              flush=True)
    print("sgpu server %s listening on :8080" % collector.SGPU_VERSION,
          flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
