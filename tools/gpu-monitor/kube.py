"""Kubernetes pod listing for sgpu.

Credential chain, re-checked on every cache refresh (an `optional: true`
Secret created after pod start is synced into the volume by kubelet within
about a minute, so the pods view lights up without a pod restart):

1. Kubeconfig file at $KUBECONFIG (default /etc/sgpu/kubeconfig/config),
   token-auth only — that is what the lab kubeconfig uses.
2. In-cluster service account (today it gets 403 in this namespace, but it
   is harmless to try and becomes correct if permissions ever appear).

Only stdlib + PyYAML (present in the image; a kubeconfig without PyYAML is
reported as an explicit error rather than half-parsed).
"""

import base64
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.request

from collector import owner_from_name

NAMESPACE = os.environ.get("POD_NAMESPACE", "p-sgvr-node-02")
NODE_NAME = os.environ.get("NODE_NAME", "")
KUBECONFIG = os.environ.get("KUBECONFIG", "/etc/sgpu/kubeconfig/config")
CACHE_TTL = float(os.environ.get("SGPU_PODS_CACHE_TTL", "45"))
SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"

_lock = threading.Lock()
_cache = {"at": 0.0, "data": None}


def _load_kubeconfig():
    """Return (server, token, ssl_context) or raise with a clear message."""
    with open(KUBECONFIG, "r", encoding="utf-8") as fh:
        text = fh.read()
    try:
        import yaml
    except ImportError:
        raise RuntimeError("kubeconfig present but PyYAML is missing")
    config = yaml.safe_load(text)
    current = config.get("current-context", "")
    context = next((c["context"] for c in config.get("contexts", [])
                    if c.get("name") == current), None)
    if context is None:
        raise RuntimeError("kubeconfig: current-context not found")
    cluster = next((c["cluster"] for c in config.get("clusters", [])
                    if c.get("name") == context.get("cluster")), None)
    user = next((u["user"] for u in config.get("users", [])
                 if u.get("name") == context.get("user")), None)
    if cluster is None or user is None:
        raise RuntimeError("kubeconfig: cluster or user entry missing")
    token = user.get("token")
    if not token:
        raise RuntimeError("kubeconfig: only token auth is supported")
    server = cluster.get("server", "").rstrip("/")
    if cluster.get("insecure-skip-tls-verify"):
        ssl_context = ssl._create_unverified_context()
    elif cluster.get("certificate-authority-data"):
        ca = base64.b64decode(cluster["certificate-authority-data"])
        ssl_context = ssl.create_default_context(
            cadata=ca.decode("utf-8", "replace"))
    else:
        ssl_context = ssl.create_default_context()
    return server, token, ssl_context


def _load_serviceaccount():
    token_path = os.path.join(SA_DIR, "token")
    with open(token_path, "r", encoding="utf-8") as fh:
        token = fh.read().strip()
    ssl_context = ssl.create_default_context(
        cafile=os.path.join(SA_DIR, "ca.crt"))
    return "https://kubernetes.default.svc", token, ssl_context


def _credentials():
    if os.path.exists(KUBECONFIG):
        return "kubeconfig", _load_kubeconfig()
    if os.path.exists(os.path.join(SA_DIR, "token")):
        return "serviceaccount", _load_serviceaccount()
    raise RuntimeError("no kubeconfig secret and no service account token")


def _api_get(path, timeout=4):
    source, (server, token, ssl_context) = _credentials()
    request = urllib.request.Request(
        server + path, headers={"Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(request, context=ssl_context,
                                    timeout=timeout) as response:
            return source, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace")
        try:
            message = json.loads(body).get("message", body)
        except ValueError:
            message = body
        raise RuntimeError("%s: HTTP %d: %s" % (source, exc.code,
                                                message[:160]))


def _age_from(start_iso, now):
    if not start_iso:
        return "?"
    try:
        from datetime import datetime, timezone
        started = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        seconds = max(0, int(now - started.timestamp()))
    except ValueError:
        return "?"
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    if days:
        return "%dd%dh" % (days, hours)
    if hours:
        return "%dh%dm" % (hours, minutes)
    return "%dm" % minutes


def _gpu_request(pod):
    total = 0
    for container in pod.get("spec", {}).get("containers", []):
        resources = container.get("resources", {})
        value = resources.get("requests", {}).get("nvidia.com/gpu") \
            or resources.get("limits", {}).get("nvidia.com/gpu") or 0
        try:
            total += int(value)
        except (TypeError, ValueError):
            pass
    return total


def get_pods():
    """Cached pod view: display rows (GPU-requesting, this node) plus a
    uid -> pod-name map over ALL pods on the node (attribution fallback)."""
    with _lock:
        now = time.time()
        if _cache["data"] is not None and now - _cache["at"] < CACHE_TTL:
            return dict(_cache["data"], uid_to_pod=dict(
                _cache["data"].get("uid_to_pod", {})))
        try:
            source, data = _api_get(
                "/api/v1/namespaces/%s/pods" % NAMESPACE)
        except Exception as exc:
            result = {"ok": False, "source": None,
                      "error": str(exc), "rows": [], "uid_to_pod": {}}
            _cache["at"] = now
            _cache["data"] = result
            return dict(result, uid_to_pod={})
        rows = []
        uid_to_pod = {}
        for pod in data.get("items", []):
            metadata = pod.get("metadata", {})
            spec = pod.get("spec", {})
            status = pod.get("status", {})
            name = metadata.get("name", "")
            node = spec.get("nodeName", "")
            if NODE_NAME and node != NODE_NAME:
                continue
            if metadata.get("uid"):
                uid_to_pod[metadata["uid"].lower()] = name
            phase = status.get("phase", "")
            gpu = _gpu_request(pod)
            if phase not in ("Running", "Pending") or gpu <= 0:
                continue
            start_iso = status.get("startTime")
            rows.append({
                "owner": owner_from_name(name) or "?",
                "pod": name,
                "node": node,
                "phase": phase,
                "gpu": gpu,
                "age": _age_from(start_iso, now),
                "uid": (metadata.get("uid") or "").lower(),
                "start_iso": start_iso,
            })
        rows.sort(key=lambda r: (r["owner"], r["pod"]))
        result = {"ok": True, "source": source, "error": None, "rows": rows,
                  "uid_to_pod": uid_to_pod}
        _cache["at"] = now
        _cache["data"] = result
        return dict(result, uid_to_pod=dict(uid_to_pod))
