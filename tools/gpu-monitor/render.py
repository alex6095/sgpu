"""Shared dashboard layout for sgpu.

Layout functions turn a schema-2 snapshot into lines of (text, tag) segments.
Two backends consume them: render_text() maps tags to ANSI SGR codes for
/table and /apps, and tui.py maps the same tags to curses color pairs.
Thresholds, bars, column widths and owner colors live here and only here.

Tags: title section header rule ok warn crit dim plain o0..o5
"""

import time
from datetime import datetime, timezone, timedelta

KST = timezone(timedelta(hours=9))

ANSI = {
    "title": "\x1b[1;36m",
    "section": "\x1b[1;35m",
    "header": "\x1b[1m",
    "rule": "\x1b[2m",
    "ok": "\x1b[32m",
    "warn": "\x1b[33m",
    "crit": "\x1b[31m",
    "dim": "\x1b[2m",
    "plain": "",
    "o0": "\x1b[96m",
    "o1": "\x1b[92m",
    "o2": "\x1b[93m",
    "o3": "\x1b[95m",
    "o4": "\x1b[94m",
    "o5": "\x1b[91m",
}
RESET = "\x1b[0m"
OWNER_TAG_COUNT = 6


def owner_tag(owner):
    if not owner or owner == "?":
        return "dim"
    code = 0
    for ch in owner:  # multiplicative hash: plain ord-sums collide often
        code = (code * 31 + ord(ch)) % 100003
    return "o%d" % (code % OWNER_TAG_COUNT)


def util_tag(util):
    if util is None:
        return "dim"
    if util >= 90:
        return "crit"
    if util >= 50:
        return "warn"
    return "ok"


def make_bar(pct, width, unicode_ok=True):
    if pct is None:
        pct = 0
    filled = int(round(max(0, min(100, pct)) / 100.0 * width))
    if unicode_ok:
        return "█" * filled + "░" * (width - filled)
    return "#" * filled + "-" * (width - filled)


def fmt_gib(mib):
    if mib is None:
        return "?"
    return "%.1fG" % (mib / 1024.0)


def fmt_uptime(started_utc, now=None):
    if not started_utc:
        return "?"
    try:
        started = datetime.fromisoformat(started_utc.replace("Z", "+00:00"))
    except ValueError:
        return "?"
    seconds = max(0, int((now or time.time()) - started.timestamp()))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    if days:
        return "%dd%dh" % (days, hours)
    if hours:
        return "%dh%dm" % (hours, minutes)
    return "%dm" % minutes


def clip(text, width):
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 1:
        return ">"
    return text[:width - 1] + ">"


def pad(text, width):
    return clip(text, width).ljust(width)


def _short_gpu_name(name):
    name = (name or "?").replace("NVIDIA ", "")
    return clip(name, 8)


def _seg_line(*segments):
    return [seg for seg in segments if seg[0]]


# --- panes -------------------------------------------------------------------


def layout_header(snapshot, width):
    try:
        utc = datetime.fromisoformat(
            snapshot["time_utc"].replace("Z", "+00:00"))
        stamp = "%s UTC (%s KST)" % (utc.strftime("%H:%M:%S"),
                                     utc.astimezone(KST).strftime("%H:%M"))
    except (KeyError, ValueError):
        stamp = "?"
    line = "SGPU  node=%s  driver=%s  %s" % (
        snapshot.get("node", "?"), snapshot.get("driver", "?"), stamp)
    return [[(clip(line, width), "title")]]


def layout_gpus(snapshot, width, unicode_ok=True):
    bar_w = 10 if width >= 96 else (8 if width >= 80 else 6)
    show_owners = width >= 84
    # Header is composed with the exact same column widths as the data rows
    # below (left 14, util span bar_w+9, mem span bar_w+17, "%4s  %9s" tail)
    # so TEMP/POWER/OWNERS always line up regardless of bar width.
    header = "%3s %-9s %-*s%-*s%4s  %9s" % (
        "GPU", "NAME", bar_w + 9, "UTIL", bar_w + 17, "MEM", "TEMP", "POWER")
    if show_owners:
        header += "  OWNERS"
    lines = [[(clip(header, width), "header")]]
    for gpu in snapshot.get("gpus", []):
        util = gpu.get("util")
        tag = util_tag(util)
        left = "%3s %-9s " % (gpu.get("index", "?"),
                              _short_gpu_name(gpu.get("name")))
        util_text = "[%s] %4s  " % (
            make_bar(util, bar_w, unicode_ok),
            ("%d%%" % util) if util is not None else "?")
        mem_used = gpu.get("mem_used_mib")
        mem_total = gpu.get("mem_total_mib")
        mem_pct = (100.0 * mem_used / mem_total) if mem_used is not None \
            and mem_total else None
        mem_text = "[%s] %-13s " % (
            make_bar(mem_pct, bar_w, unicode_ok),
            "%s/%s" % (fmt_gib(mem_used), fmt_gib(mem_total)))
        temp = gpu.get("temp_c")
        power = gpu.get("power_w")
        limit = gpu.get("power_limit_w")
        tail = "%4s  %9s" % (
            ("%dC" % temp) if temp is not None else "?",
            "%s/%sW" % ("%d" % power if power is not None else "?",
                        "%d" % limit if limit is not None else "?"))
        segments = [(left, tag), (util_text, tag), (mem_text, tag),
                    (tail, tag)]
        if show_owners:
            owners = gpu.get("owners") or []
            segments.append(("  ", "plain"))
            if owners:
                for pos, owner in enumerate(owners):
                    if pos:
                        segments.append((",", "plain"))
                    segments.append((owner, owner_tag(owner)))
            else:
                segments.append(("-", "dim"))
        lines.append(segments)
    if not snapshot.get("gpus"):
        lines.append([(snapshot.get("error", "no GPUs reported"), "warn")])
    return lines


def proc_columns(width):
    """Column widths for the process table at a given terminal width."""
    fixed = 3 + 2 + 8 + 2 + 7 + 2 + 4 + 2 + 7 + 2 + 7 + 2  # all but POD/CMD
    flexible = max(20, width - fixed)
    pod_w = max(12, min(42, flexible - 16))
    cmd_w = max(4, flexible - pod_w)
    return pod_w, cmd_w


def layout_procs(snapshot, width, now=None):
    """Returns {"header": [...], "rows": [...]} so the TUI can scroll rows."""
    pod_w, cmd_w = proc_columns(width)
    title = [(clip("NVIDIA compute processes", width), "section")]
    head = [("%3s  %-8s  %-*s  %7s  %4s  %7s  %7s  %s" % (
        "GPU", "OWNER", pod_w, "POD", "PID", "SM%", "MEM", "UP", "CMD"),
        "header")]
    rows = []
    for proc in snapshot.get("procs", []):
        owner = proc.get("owner") or "?"
        sm = proc.get("sm_util")
        rows.append(_seg_line(
            ("%3s  " % (proc.get("gpu_index")
                        if proc.get("gpu_index") is not None else "?"),
             "plain"),
            (pad(owner, 8), owner_tag(proc.get("owner"))),
            ("  %s  " % pad(proc.get("pod") or "?", pod_w),
             owner_tag(proc.get("owner"))),
            ("%7s  " % proc.get("pid", "?"), "plain"),
            ("%4s  " % (("%d" % sm) if sm is not None else "-"),
             util_tag(sm) if sm is not None else "dim"),
            ("%7s  " % fmt_gib(proc.get("mem_mib")), "plain"),
            ("%7s  " % fmt_uptime(proc.get("started_utc"), now), "dim"),
            (clip(proc.get("cmd") or "?", cmd_w), "dim"),
        ))
    if not rows:
        rows.append([("(no compute processes)", "dim")])
    return {"header": [title, head], "rows": rows}


def fmt_tib(nbytes):
    if nbytes is None:
        return "?"
    return "%.1fT" % (nbytes / float(1024 ** 4))


def layout_storage(snapshot, width, unicode_ok=True):
    storage = snapshot.get("storage")
    if not storage:
        return []
    pct = storage.get("pct") or 0.0
    tag = "crit" if pct >= 95 else ("warn" if pct >= 85 else "ok")
    bar_w = 10 if width >= 96 else 8
    line = _seg_line(
        ("STORAGE %-11s " % clip(storage.get("label", "?"), 11), "header"),
        ("[%s] %4.1f%%  " % (make_bar(pct, bar_w, unicode_ok), pct), tag),
        ("%s/%s used, %s free" % (fmt_tib(storage.get("used_bytes")),
                                  fmt_tib(storage.get("total_bytes")),
                                  fmt_tib(storage.get("free_bytes"))), tag),
    )
    return [line]


def layout_pods(snapshot, width):
    pods = snapshot.get("pods") or {}
    title = [(clip("Kubernetes GPU pods on this node", width), "section")]
    if not pods.get("ok"):
        hint = ("pod allocation view disabled (%s) — create the "
                "sgpu-kubeconfig secret, see README" %
                (pods.get("error") or "no credentials"))
        return {"header": [title], "rows": [[(clip(hint, width), "dim")]]}
    head = [("%-8s  %3s  %3s  %-7s  %-8s  %s" % (
        "OWNER", "REQ", "ACT", "AGE", "PHASE", "POD"), "header")]
    rows = []
    for row in pods.get("rows", []):
        idle = row.get("phase") == "Running" and row.get("active", 0) == 0
        pod_tag = "warn" if idle else owner_tag(row.get("owner"))
        rows.append(_seg_line(
            (pad(row.get("owner") or "?", 8), owner_tag(row.get("owner"))),
            ("  %3s  %3s  %-7s  %-8s  " % (
                row.get("gpu", "?"), row.get("active", 0),
                row.get("age", "?"), row.get("phase", "?")), "plain"),
            (clip(row.get("pod") or "?", max(10, width - 40)), pod_tag),
            ("  IDLE" if idle else "", "warn"),
        ))
    if not rows:
        rows.append([("(no Running/Pending GPU-requesting pods)", "dim")])
    return {"header": [title, head], "rows": rows}


# --- text backend ------------------------------------------------------------


def paint(segments, color):
    if not color:
        return "".join(text for text, _ in segments)
    parts = []
    for text, tag in segments:
        code = ANSI.get(tag, "")
        parts.append(code + text + (RESET if code else ""))
    return "".join(parts)


def render_text(snapshot, width=120, color=False, unicode_ok=True,
                footer_notes=()):
    lines = []
    lines.extend(layout_header(snapshot, width))
    lines.append([])
    lines.extend(layout_gpus(snapshot, width, unicode_ok))
    lines.append([])
    procs = layout_procs(snapshot, width)
    lines.extend(procs["header"])
    lines.extend(procs["rows"])
    lines.append([])
    pods = layout_pods(snapshot, width)
    lines.extend(pods["header"])
    lines.extend(pods["rows"])
    storage = layout_storage(snapshot, width, unicode_ok)
    if storage:
        lines.append([])
        lines.extend(storage)
    for note in footer_notes:
        lines.append([(clip(note, width), "dim")])
    return "\n".join(paint(line, color) for line in lines) + "\n"


def render_procs_text(snapshot, width=120, color=False):
    procs = layout_procs(snapshot, width)
    lines = procs["header"] + procs["rows"]
    return "\n".join(paint(line, color) for line in lines) + "\n"
