import csv
import json
import os
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


GPU_FIELDS = [
    "index",
    "uuid",
    "name",
    "temperature.gpu",
    "utilization.gpu",
    "memory.total",
    "memory.used",
    "memory.free",
    "power.draw",
    "power.limit",
]

APP_FIELDS = [
    "gpu_uuid",
    "pid",
    "process_name",
    "used_memory",
]

CACHE = {}
CACHE_TTL_SECONDS = float(os.environ.get("GPU_MONITOR_CACHE_TTL", "1.0"))
NODE_NAME = os.environ.get("NODE_NAME", "")
NAMESPACE = os.environ.get("POD_NAMESPACE", "p-sgvr-node-02")


def run(cmd, timeout=5):
    try:
        proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
        return {
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    except Exception as exc:
        return {
            "ok": False,
            "returncode": -1,
            "stdout": "",
            "stderr": f"{type(exc).__name__}: {exc}",
        }


def query_csv(kind, fields):
    result = run([
        "nvidia-smi",
        f"--query-{kind}=" + ",".join(fields),
        "--format=csv,noheader,nounits",
    ])
    rows = []
    if result["ok"]:
        for row in csv.reader(result["stdout"].splitlines()):
            if row:
                rows.append({key: value.strip() for key, value in zip(fields, row)})
    result["rows"] = rows
    return result


def kube_api(path, timeout=3):
    token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    if not os.path.exists(token_path):
        return {"ok": False, "error": "service account token is not mounted"}
    with open(token_path, "r", encoding="utf-8") as token_file:
        token = token_file.read().strip()
    url = f"https://kubernetes.default.svc{path}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    context = ssl.create_default_context(cafile=ca_path)
    try:
        with urllib.request.urlopen(req, context=context, timeout=timeout) as response:
            return {"ok": True, "data": json.loads(response.read().decode("utf-8"))}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            message = json.loads(body).get("message", body)
        except json.JSONDecodeError:
            message = body
        return {"ok": False, "error": f"HTTP {exc.code}: {message[:180]}"}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


def parse_int(value, default=0):
    try:
        return int(str(value).strip())
    except Exception:
        return default


def age_from_start(start_time):
    if not start_time:
        return "?"
    try:
        started = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    except ValueError:
        return "?"
    seconds = max(0, int((datetime.now(timezone.utc) - started).total_seconds()))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    if days:
        return f"{days}d{hours}h"
    if hours:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"


def owner_from_name(name):
    if not name:
        return "?"
    normalized = name.replace("_", "-")
    return normalized.split("-", 1)[0]


def gpu_request_for_pod(pod):
    total = 0
    for container in pod.get("spec", {}).get("containers", []):
        requests = container.get("resources", {}).get("requests", {})
        limits = container.get("resources", {}).get("limits", {})
        total += parse_int(requests.get("nvidia.com/gpu", limits.get("nvidia.com/gpu", 0)))
    return total


def list_gpu_pods():
    result = kube_api(f"/api/v1/namespaces/{NAMESPACE}/pods")
    if not result["ok"]:
        return {"ok": False, "error": result["error"], "rows": []}
    rows = []
    for pod in result["data"].get("items", []):
        phase = pod.get("status", {}).get("phase", "")
        if phase not in ("Running", "Pending"):
            continue
        gpu = gpu_request_for_pod(pod)
        if gpu <= 0:
            continue
        node = pod.get("spec", {}).get("nodeName", "")
        if NODE_NAME and node != NODE_NAME:
            continue
        name = pod.get("metadata", {}).get("name", "")
        rows.append({
            "owner": owner_from_name(name),
            "pod": name,
            "node": node,
            "phase": phase,
            "gpu": gpu,
            "age": age_from_start(pod.get("status", {}).get("startTime")),
        })
    rows.sort(key=lambda row: (row["owner"], row["pod"]))
    return {"ok": True, "rows": rows}


def snapshot(force=False):
    now = time.time()
    cached_at = CACHE.get("time", 0)
    if not force and now - cached_at < CACHE_TTL_SECONDS:
        return CACHE["data"]
    data = {
        "time": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "node": NODE_NAME or "?",
        "gpus": query_csv("gpu", GPU_FIELDS),
        "apps": query_csv("compute-apps", APP_FIELDS),
        "pods": list_gpu_pods(),
    }
    CACHE["time"] = now
    CACHE["data"] = data
    return data


def fmt(value, suffix="", width=0):
    text = "?" if value in ("[Not Supported]", "N/A", "") else str(value)
    if suffix and text != "?":
        text += suffix
    return text.rjust(width) if width else text


def render_table(data):
    lines = []
    lines.append(f"MLXP GPU monitor  node={data['node']}  time={data['time']}")
    lines.append("")
    lines.append("GPU  Name        Used/Total MiB        Util  Temp  Power")
    lines.append("---  ----------  --------------------  ----  ----  --------")
    if data["gpus"]["ok"]:
        for row in data["gpus"]["rows"]:
            used = fmt(row.get("memory.used"), width=6)
            total = fmt(row.get("memory.total"), width=6)
            util = fmt(row.get("utilization.gpu"), "%", 5)
            temp = fmt(row.get("temperature.gpu"), "C", 5)
            power = f"{fmt(row.get('power.draw'), width=5)}/{fmt(row.get('power.limit'))}W"
            lines.append(
                f"{row.get('index', '?'):>3}  {row.get('name', '?'):<10}  "
                f"{used}/{total}          {util}  {temp}  {power}"
            )
    else:
        lines.append(data["gpus"]["stderr"] or "nvidia-smi query failed")

    pods = data["pods"]
    if pods["ok"]:
        lines.append("")
        lines.append("Kubernetes GPU pods on this node")
        lines.append("OWNER    GPU  AGE     PHASE    POD")
        lines.append("-------  ---  ------  -------  ----------------------------------------")
        if pods["rows"]:
            for row in pods["rows"]:
                lines.append(
                    f"{row['owner'][:7]:<7}  {row['gpu']:>3}  {row['age']:<6}  "
                    f"{row['phase']:<7}  {row['pod']}"
                )
        else:
            lines.append("(no Running/Pending GPU-requesting pods visible)")

    lines.append("")
    apps = data["apps"]
    if apps["ok"] and apps["rows"]:
        lines.append("NVIDIA compute processes")
        lines.append("GPU UUID                              PID      MEM     NAME")
        lines.append("------------------------------------  -------  ------  ----------------")
        for row in apps["rows"]:
            lines.append(
                f"{row.get('gpu_uuid', '?'):<36}  {row.get('pid', '?'):>7}  "
                f"{row.get('used_memory', '?'):>6}  {row.get('process_name', '?')}"
            )
    else:
        lines.append("NVIDIA compute processes: none reported by nvidia-smi in this container.")
        lines.append("Use the Kubernetes pod list above as the reliable owner/pod view.")
    return "\n".join(lines) + "\n"


def render_apps(data):
    lines = []
    apps = data["apps"]
    if apps["ok"] and apps["rows"]:
        for row in apps["rows"]:
            lines.append(json.dumps(row, ensure_ascii=False))
    else:
        lines.append("No compute processes reported by nvidia-smi.")
    lines.append("")
    lines.append("GPU-requesting pods:")
    pods = data["pods"]
    if pods["ok"]:
        for row in pods["rows"]:
            lines.append(f"{row['owner']} gpu={row['gpu']} age={row['age']} phase={row['phase']} pod={row['pod']}")
    else:
        lines.append("Pod list unavailable from monitor pod; local sgpu adds this via kubectl.")
    return "\n".join(lines) + "\n"


class Handler(BaseHTTPRequestHandler):
    def send_body(self, code, body, content_type="text/plain; charset=utf-8"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/table", "/short"):
            self.send_body(200, render_table(snapshot()))
            return
        if path == "/health":
            result = run(["nvidia-smi", "-L"])
            self.send_body(200 if result["ok"] else 503, result["stdout"] or result["stderr"])
            return
        if path == "/smi":
            result = run(["nvidia-smi"], timeout=8)
            self.send_body(200 if result["ok"] else 503, result["stdout"] + result["stderr"])
            return
        if path == "/apps":
            self.send_body(200, render_apps(snapshot()))
            return
        if path == "/pods":
            pods = snapshot()["pods"]
            self.send_body(200 if pods["ok"] else 503, json.dumps(pods, indent=2, ensure_ascii=False), "application/json")
            return
        if path == "/gpustat":
            result = run(["gpustat", "--no-color"], timeout=5)
            self.send_body(200 if result["ok"] else 503, result["stdout"] + result["stderr"])
            return
        if path == "/topo":
            result = run(["nvidia-smi", "topo", "-m"], timeout=8)
            self.send_body(200 if result["ok"] else 503, result["stdout"] + result["stderr"])
            return
        if path == "/json":
            self.send_body(200, json.dumps(snapshot(), indent=2, ensure_ascii=False), "application/json")
            return
        self.send_body(404, "not found\n")

    def log_message(self, fmt_string, *args):
        print("%s - %s" % (self.address_string(), fmt_string % args), flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
