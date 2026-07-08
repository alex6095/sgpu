"""sgpu interactive TUI (runs inside the monitor pod).

Launched by clients as `kubectl exec -it <pod> -- python3 /opt/gpu-monitor/tui.py
[interval]`. The refresh loop runs in-pod, so remote clients get smooth
updates with zero per-frame kubectl round trips. Data comes from the local
server's /json endpoint (one NVML consumer for the whole pod); if the server
is unreachable (dev machines, mock mode) it falls back to calling collector
directly.

stdlib only: curses + urllib. Layout and colors are shared with the text
backend through render.py style tags.
"""

import json
import locale
import os
import sys
import threading
import time
import urllib.request
from datetime import date as _date, datetime, timedelta

# kubectl exec often lands us with TERM=xterm (8 colors), which drops the
# stats grid to a monochrome ramp. Every terminal we target (WSL, Windows
# Terminal, iTerm, ...) speaks 256 colors, and the pod ships the
# xterm-256color terminfo, so upgrade basic variants to get the green scale.
_term = os.environ.get("TERM", "")
if "256" not in _term and _term in (
        "", "xterm", "ansi", "vt100", "vt220", "linux", "screen", "tmux"):
    os.environ["TERM"] = "xterm-256color"

import render

JSON_URL = os.environ.get("SGPU_JSON_URL", "http://127.0.0.1:8080/json")
STATS_URL = os.environ.get(
    "SGPU_STATS_URL", "http://127.0.0.1:8080/stats")
DEFAULT_INTERVAL = float(os.environ.get("SGPU_TUI_INTERVAL", "2"))

SORT_MODES = ("gpu", "mem", "owner")

# STATS view: axis modes and their fetch windows (days).
STATS_MODES = ("hours", "days", "weeks", "months")
STATS_MODE_DAYS = {"hours": 14, "days": 42, "weeks": 182, "months": 365}
STATS_MODE_LABEL = {"hours": "HOURS", "days": "DAYS",
                    "weeks": "WEEKS", "months": "MONTHS"}

# GitHub-contribution green scale (256-color xterm indices), zero-bucket first.
GRID_COLORS = (237, 22, 28, 34, 40, 46)  # index 0 == empty cell, 1..5 hotter
# curses color-pair numbers for the grid; start well past build_tag_attrs()'s
# pairs (it registers ~12) so we never collide. Registered lazily, once.
GRID_PAIR_BASE = 40

# Two-char cell ramps (index 0..5). Unicode preferred, ASCII fallback.
CELL_RAMP_UNICODE = ("  ", "..", "░░", "▒▒", "▓▓", "██")
CELL_RAMP_ASCII = ("  ", "..", "--", "==", "**", "##")


# --- pure aggregation helpers (no curses) -----------------------------------
# These are deliberately import-free of curses so they can be unit-tested.


def parse_ymd(date_str):
    """Parse a 'YYYYMMDD' string to a datetime.date, or None if malformed."""
    try:
        return datetime.strptime(str(date_str), "%Y%m%d").date()
    except (ValueError, TypeError):
        return None


def fill_daily_gaps(daily, window_days, today=None):
    """Return a contiguous list of (date, owners_dict) for the last
    `window_days` calendar days ending at `today`, filling missing dates with
    empty owner maps.

    `daily` is the server's list of {"date": "YYYYMMDD", "owners": {...}} in
    oldest->newest order (only dates with data). `today` defaults to the max
    date present in `daily` (or the real today if `daily` is empty), so the
    range always ends on the most recent day we know about.
    """
    if today is None:
        seen = [parse_ymd(d.get("date")) for d in (daily or [])]
        seen = [d for d in seen if d is not None]
        today = max(seen) if seen else _date.today()
    by_date = {}
    for entry in (daily or []):
        d = parse_ymd(entry.get("date"))
        if d is not None:
            by_date[d] = entry.get("owners") or {}
    span = max(1, int(window_days))
    start = today - timedelta(days=span - 1)
    out = []
    cur = start
    while cur <= today:
        out.append((cur, by_date.get(cur, {})))
        cur += timedelta(days=1)
    return out


def bucket_by_week(filled):
    """Aggregate a filled daily series into ISO weeks (Monday start).

    Input: list of (date, owners_dict) contiguous days.
    Output: list of (week_start_date, owners_dict) contiguous ISO weeks,
    oldest->newest, with owner gpu_seconds summed within each week.
    """
    weeks = {}
    order = []
    for d, owners in filled:
        wk = d - timedelta(days=d.weekday())  # Monday of that ISO week
        if wk not in weeks:
            weeks[wk] = {}
            order.append(wk)
        acc = weeks[wk]
        for owner, secs in (owners or {}).items():
            acc[owner] = acc.get(owner, 0.0) + (secs or 0.0)
    order.sort()
    # Fill any missing weeks in the (min..max) span so columns are contiguous.
    if not order:
        return []
    out = []
    cur = order[0]
    last = order[-1]
    while cur <= last:
        out.append((cur, weeks.get(cur, {})))
        cur += timedelta(days=7)
    return out


def bucket_by_month(filled):
    """Aggregate a filled daily series into calendar months.

    Output: list of (first_of_month_date, owners_dict) contiguous months,
    oldest->newest.
    """
    months = {}
    order = []
    for d, owners in filled:
        key = (d.year, d.month)
        if key not in months:
            months[key] = {}
            order.append(key)
        acc = months[key]
        for owner, secs in (owners or {}).items():
            acc[owner] = acc.get(owner, 0.0) + (secs or 0.0)
    order.sort()
    if not order:
        return []
    out = []
    (y, m) = order[0]
    (ly, lm) = order[-1]
    while (y, m) <= (ly, lm):
        out.append((_date(y, m, 1), months.get((y, m), {})))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def bucketize(value, row_max):
    """Map a value onto the 0..5 intensity scale for its row.

    0 iff value<=0 (or row has no positive max). Otherwise 1..5 by fraction of
    row_max in equal fifths: (0,0.2]->1, (0.2,0.4]->2, ... (0.8,1.0]->5.
    """
    if value is None or value <= 0 or not row_max or row_max <= 0:
        return 0
    frac = float(value) / float(row_max)
    if frac >= 1.0:
        return 5
    level = int(frac * 5.0) + 1
    if level < 1:
        return 1
    if level > 5:
        return 5
    return level


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


def update_banner_segments(snapshot):
    """Yellow upgrade banner when the launching client (SGPU_CLIENT_VERSION)
    is behind this server's version. Returns segments or None."""
    client = os.environ.get("SGPU_CLIENT_VERSION")
    server = (snapshot or {}).get("sgpu_version")
    if not client or not server:
        return None
    try:
        if _version_tuple(client) >= _version_tuple(server):
            return None
    except Exception:
        return None
    return [("↑ update available: ", "warn"),
            ("sgpu %s" % server, "header"),
            (" (you have %s) — run: " % client, "dim"),
            ("pip install -U sgpu", "warn")]


def owner_series_from_columns(columns, owner):
    """Extract one owner's per-column values from a list of (label, owners)."""
    return [(owners or {}).get(owner, 0.0) for _label, owners in columns]


def total_series_from_columns(columns):
    """Sum all owners' values per column -> one list of totals."""
    return [sum((owners or {}).values()) for _label, owners in columns]


def top_owners_by_total(merged_owners, limit=None):
    """Owners sorted by gpu_seconds desc; optionally truncated to `limit`."""
    ranked = sorted(
        (merged_owners or {}).items(),
        key=lambda kv: -(kv[1].get("gpu_seconds", 0.0) if kv[1] else 0.0))
    names = [name for name, _acc in ranked]
    if limit is not None:
        names = names[:limit]
    return names


def hour_columns(merged_owners):
    """24 KST-hour columns from per-owner hour_hist_kst.

    Returns (columns, ticks) where columns is a list of 24 (label, owners)
    with owners mapping owner->that hour's gpu-seconds, and ticks is a list of
    24 strings (2 chars, only every 3rd hour labeled) aligned under the cells.
    """
    columns = []
    for hh in range(24):
        owners = {}
        for owner, acc in (merged_owners or {}).items():
            hist = (acc or {}).get("hour_hist_kst") or []
            if hh < len(hist):
                owners[owner] = hist[hh] or 0.0
        columns.append((str(hh), owners))
    ticks = []
    for hh in range(24):
        ticks.append(("%d" % hh).ljust(2) if hh % 3 == 0 else "  ")
    return columns, ticks


def day_columns(daily, window_days, today=None):
    """Contiguous per-day columns; ticks MM/DD every 7th cell (2-char cells)."""
    filled = fill_daily_gaps(daily, window_days, today)
    columns = [(d.strftime("%Y%m%d"), owners) for d, owners in filled]
    ticks = _spaced_ticks(
        [d for d, _ in filled], lambda d: d.strftime("%m/%d"), every=7)
    return columns, ticks


def week_columns(daily, window_days, today=None):
    """Contiguous ISO-week columns; ticks MM/DD (week start) every 4th cell."""
    filled = fill_daily_gaps(daily, window_days, today)
    weeks = bucket_by_week(filled)
    columns = [(d.strftime("%Y%m%d"), owners) for d, owners in weeks]
    ticks = _spaced_ticks(
        [d for d, _ in weeks], lambda d: d.strftime("%m/%d"), every=4)
    return columns, ticks


def month_columns(daily, window_days, today=None, tight=False):
    """Contiguous calendar-month columns; ticks YYYY-MM (or MM if tight)
    every 3rd cell."""
    filled = fill_daily_gaps(daily, window_days, today)
    months = bucket_by_month(filled)
    columns = [(d.strftime("%Y%m"), owners) for d, owners in months]
    fmt = (lambda d: d.strftime("%m")) if tight \
        else (lambda d: d.strftime("%Y-%m"))
    ticks = _spaced_ticks([d for d, _ in months], fmt, every=3)
    return columns, ticks


def _spaced_ticks(dates, fmt, every):
    """Build a list of 2-char tick strings, one per column, labeling every
    `every`th column with fmt(date) laid across the cells to its right.

    Cells are 2 chars wide, so an N-char label spans ceil(N/2) columns. We
    write the label starting at the labeled column and blank the columns it
    overruns so labels never overlap.
    """
    n = len(dates)
    ticks = ["  "] * n
    i = 0
    while i < n:
        label = fmt(dates[i])
        span = (len(label) + 1) // 2  # columns this label consumes (2 ch each)
        padded = label.ljust(span * 2)
        for j in range(span):
            if i + j < n:
                ticks[i + j] = padded[j * 2:j * 2 + 2]
        i += max(every, span)
    return ticks


class Fetcher(threading.Thread):
    def __init__(self, interval):
        super().__init__(daemon=True)
        self.interval = interval
        self.lock = threading.Lock()
        self.snapshot = None
        self.error = None
        self.fetched_at = 0.0
        self.generation = 0
        self.wake = threading.Event()

    def fetch_once(self):
        try:
            with urllib.request.urlopen(JSON_URL, timeout=5) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data, None
        except Exception as http_exc:
            try:
                import collector
                return collector.collect(), None
            except Exception:
                return None, "cannot reach sgpu server: %s" % http_exc

    def run(self):
        while True:
            snapshot, error = self.fetch_once()
            with self.lock:
                if snapshot is not None:
                    self.snapshot = snapshot
                    self.generation += 1
                self.error = error
                self.fetched_at = time.time()
            self.wake.wait(self.interval)
            self.wake.clear()

    def poke(self):
        self.wake.set()

    def state(self):
        with self.lock:
            return self.snapshot, self.error, self.fetched_at, self.generation


class View:
    def __init__(self):
        self.focus = "procs"          # procs | pods
        self.selected = {"procs": 0, "pods": 0}
        self.offset = {"procs": 0, "pods": 0}
        self.sort = "gpu"
        self.owner_filter = None
        self.paused = False
        self.screen = "dash"          # dash | stats

    def visible_procs(self, snapshot):
        procs = list(snapshot.get("procs", []))
        if self.owner_filter:
            procs = [p for p in procs
                     if (p.get("owner") or "?") == self.owner_filter]
        if self.sort == "mem":
            procs.sort(key=lambda p: -(p.get("mem_mib") or 0))
        elif self.sort == "owner":
            procs.sort(key=lambda p: ((p.get("owner") or "~"),
                                      p.get("gpu_index") or 0))
        return procs

    def visible_pods(self, snapshot):
        rows = list((snapshot.get("pods") or {}).get("rows", []))
        if self.owner_filter:
            rows = [r for r in rows
                    if (r.get("owner") or "?") == self.owner_filter]
        return rows

    def owners(self, snapshot):
        names = set()
        for proc in snapshot.get("procs", []):
            names.add(proc.get("owner") or "?")
        for row in (snapshot.get("pods") or {}).get("rows", []):
            names.add(row.get("owner") or "?")
        return sorted(names)

    def cycle_owner(self, snapshot):
        owners = self.owners(snapshot)
        if not owners:
            self.owner_filter = None
            return
        if self.owner_filter not in owners:
            self.owner_filter = owners[0]
        else:
            index = owners.index(self.owner_filter) + 1
            self.owner_filter = None if index >= len(owners) else owners[index]

    def move(self, delta, row_count):
        pane = self.focus
        self.selected[pane] = max(0, min(max(0, row_count - 1),
                                         self.selected[pane] + delta))

    def clamp(self, pane, row_count, view_height):
        self.selected[pane] = max(0, min(max(0, row_count - 1),
                                         self.selected[pane]))
        if view_height <= 0:
            self.offset[pane] = 0
            return
        if self.selected[pane] < self.offset[pane]:
            self.offset[pane] = self.selected[pane]
        if self.selected[pane] >= self.offset[pane] + view_height:
            self.offset[pane] = self.selected[pane] - view_height + 1
        max_offset = max(0, row_count - view_height)
        self.offset[pane] = max(0, min(max_offset, self.offset[pane]))


class StatsFetcher(threading.Thread):
    """Background fetcher for the /stats endpoint.

    Unlike the live Fetcher, this does not poll on a timer: it fetches on
    demand (per axis mode) and caches each mode's payload. The UI thread calls
    request(days) which wakes the worker; results land in `cache[days]`.
    Reuses the daemon-thread + Event pattern so the UI loop never blocks.
    """

    def __init__(self):
        super().__init__(daemon=True)
        self.lock = threading.Lock()
        self.cache = {}          # (days, scope) -> {"data","error","fetched_at"}
        self.pending = set()     # (days, scope) keys awaiting a fetch
        self.inflight = set()    # (days, scope) currently being fetched
        self.wake = threading.Event()

    def fetch_once(self, days, scope):
        url = "%s?days=%d&format=json" % (STATS_URL, int(days))
        if scope == "lab":
            url += "&scope=lab"
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                data = json.loads(response.read().decode("utf-8"))
            return data, None
        except Exception as exc:
            return None, "cannot reach sgpu stats: %s" % exc

    def run(self):
        while True:
            self.wake.wait()
            self.wake.clear()
            while True:
                with self.lock:
                    todo = sorted(self.pending)
                    self.pending = set()
                    for k in todo:
                        self.inflight.add(k)
                if not todo:
                    break
                for key in todo:
                    days, scope = key
                    data, error = self.fetch_once(days, scope)
                    with self.lock:
                        self.inflight.discard(key)
                        prev = self.cache.get(key)
                        if data is not None:
                            self.cache[key] = {
                                "data": data, "error": None,
                                "fetched_at": time.time()}
                        else:
                            # keep stale data if we had some; record the error
                            self.cache[key] = {
                                "data": prev.get("data") if prev else None,
                                "error": error, "fetched_at": time.time()}

    def request(self, days, scope="local", force=False):
        """Queue a fetch for (days, scope) if not cached (or force=True)."""
        key = (int(days), scope)
        with self.lock:
            have = key in self.cache and self.cache[key].get("data")
            if have and not force:
                return
            self.pending.add(key)
        self.wake.set()

    def loading(self, days, scope="local"):
        key = (int(days), scope)
        with self.lock:
            return key in self.inflight or key in self.pending

    def get(self, days, scope="local"):
        with self.lock:
            entry = self.cache.get((int(days), scope))
            if not entry:
                return None, None, 0.0
            return entry.get("data"), entry.get("error"), \
                entry.get("fetched_at", 0.0)


class StatsView:
    """Scroll/axis state for the interactive STATS screen."""

    def __init__(self):
        self.mode = "hours"      # one of STATS_MODES
        self.offset = 0          # owner-row scroll within the grid
        self.scope = "local"     # "local" (this node) or "lab" (all nodes)

    def days(self):
        return STATS_MODE_DAYS[self.mode]

    def toggle_scope(self):
        self.scope = "lab" if self.scope == "local" else "local"
        self.offset = 0

    def cycle(self):
        self.mode = STATS_MODES[
            (STATS_MODES.index(self.mode) + 1) % len(STATS_MODES)]
        self.offset = 0

    def set_mode(self, mode):
        if mode in STATS_MODES and mode != self.mode:
            self.mode = mode
            self.offset = 0

    def scroll(self, delta, row_count, view_height):
        max_off = max(0, row_count - max(1, view_height))
        self.offset = max(0, min(max_off, self.offset + delta))


def build_tag_attrs(curses):
    attrs = {"plain": 0}
    if curses.has_colors():
        curses.start_color()
        try:
            curses.use_default_colors()
            background = -1
        except curses.error:
            background = curses.COLOR_BLACK
        palette = {
            "ok": curses.COLOR_GREEN, "warn": curses.COLOR_YELLOW,
            "crit": curses.COLOR_RED, "title": curses.COLOR_CYAN,
            "section": curses.COLOR_MAGENTA,
            "o0": curses.COLOR_CYAN, "o1": curses.COLOR_GREEN,
            "o2": curses.COLOR_YELLOW, "o3": curses.COLOR_MAGENTA,
            "o4": curses.COLOR_BLUE, "o5": curses.COLOR_RED,
        }
        for pair_number, (tag, color) in enumerate(palette.items(), start=1):
            try:
                curses.init_pair(pair_number, color, background)
                attrs[tag] = curses.color_pair(pair_number)
            except curses.error:
                attrs[tag] = 0
        attrs["title"] |= curses.A_BOLD
        attrs["section"] |= curses.A_BOLD
        for tag in ("o0", "o1", "o2", "o3", "o4", "o5"):
            attrs[tag] |= curses.A_BOLD
    attrs["header"] = curses.A_BOLD
    attrs["rule"] = curses.A_DIM
    attrs["dim"] = curses.A_DIM
    return attrs


class GridPainter:
    """Renders 2-char grid cells for the stats heatmap.

    Two rendering strategies, chosen once at construction:
      * 256-color terminals  -> background-colored spaces (GitHub green scale),
        via lazily-registered curses color pairs numbered from GRID_PAIR_BASE
        so they never collide with build_tag_attrs()'s pairs.
      * otherwise             -> a doubled text ramp (unicode blocks or ascii).

    cell(level) returns (text, attr) ready for stdscr.addstr.
    """

    def __init__(self, curses, unicode_ok):
        self.curses = curses
        self.attrs = [0, 0, 0, 0, 0, 0]
        self.mode = "text"          # "bg256" | "greentext" | "text"
        self.ramp = CELL_RAMP_UNICODE if unicode_ok else CELL_RAMP_ASCII
        try:
            has = curses.has_colors()
        except Exception:
            has = False
        if not has:
            return
        try:
            bg = -1
            try:
                curses.use_default_colors()
            except curses.error:
                bg = curses.COLOR_BLACK
            if curses.COLORS >= 256:
                # Exact GitHub-green background cells.
                for i, col in enumerate(GRID_COLORS):
                    pair = GRID_PAIR_BASE + i
                    curses.init_pair(pair, col, col)
                    self.attrs[i] = curses.color_pair(pair)
                self.mode = "bg256"
            else:
                # 8/16-color fallback: green foreground ramp (still green, and
                # still a gradient via dim/normal/bold) instead of white blocks.
                curses.init_pair(GRID_PAIR_BASE, curses.COLOR_GREEN, bg)
                green = curses.color_pair(GRID_PAIR_BASE)
                self.attrs = [0, green | curses.A_DIM, green | curses.A_DIM,
                              green, green, green | curses.A_BOLD]
                self.mode = "greentext"
        except curses.error:
            self.mode = "text"

    def cell(self, level):
        level = 0 if level < 0 else (5 if level > 5 else level)
        if self.mode == "bg256":
            return ("  ", self.attrs[level])
        if self.mode == "greentext":
            return (self.ramp[level], self.attrs[level])
        return (self.ramp[level], 0)


def draw_cells(stdscr, curses, y, x, levels, painter, width):
    """Draw a run of 2-char cells starting at (y, x). Returns the next x."""
    for level in levels:
        if x + 2 > width:
            break
        text, attr = painter.cell(level)
        try:
            stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass
        x += 2
    return x


def draw_segments(stdscr, curses, y, segments, attrs, width, reverse=False):
    x = 0
    for text, tag in segments:
        if x >= width:
            break
        text = text[:width - x]
        attr = attrs.get(tag, 0)
        if reverse:
            attr |= curses.A_REVERSE
        try:
            stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass  # bottom-right cell write raises harmlessly
        x += len(text)


def unicode_ok():
    encoding = getattr(sys.stdout, "encoding", None) or ""
    try:
        "█░".encode(encoding or "ascii")
        return True
    except (UnicodeEncodeError, LookupError):
        return False


def _fmt_num(value, spec="%.1f"):
    return spec % value


def _owner_node_label(owner, owner_nodes):
    """One node label if >=95% of the owner's time is there, else 'both'."""
    per_node = (owner_nodes or {}).get(owner) or {}
    total = sum(per_node.values())
    if total <= 0:
        return ""
    label, best = max(per_node.items(), key=lambda kv: kv[1])
    return label if best >= 0.95 * total else "both"


def _leaderboard_rows(owners, width, owner_nodes=None):
    """Build (header_segments, [row_segments]) for the leaderboard.

    Columns: OWNER (colored) [NODE in lab scope] GPU-H EFF-H AVG-SM%
    PEAK-MEM. Numbers right-aligned; blank when the underlying weight is
    missing.
    """
    ow = 8
    if owners:
        ow = max(8, min(20, max(len(str(o)) for o in owners)))
    ranked = sorted(owners.items(),
                    key=lambda kv: -(kv[1].get("gpu_seconds", 0.0)
                                     if kv[1] else 0.0))
    node_col = " %-7s" % "NODE" if owner_nodes is not None else ""
    header = [("%-3s %-*s%s %7s %7s %8s %9s"
               % ("#", ow, "OWNER", node_col, "GPU-H", "EFF-H", "AVG-SM%",
                  "PEAK-MEM"),
               "header")]
    rows = []
    for rank, (owner, acc) in enumerate(ranked, start=1):
        acc = acc or {}
        gpu_h = acc.get("gpu_seconds", 0.0) / 3600.0
        sm_w = acc.get("sm_weight", 0.0)
        util_w = acc.get("util_weight", 0.0)
        if sm_w:
            avg = acc.get("sm_wsum", 0.0) / sm_w
        elif util_w:
            avg = acc.get("util_wsum", 0.0) / util_w
        else:
            avg = None
        eff_s = ("%.1f" % (gpu_h * (avg / 100.0))) if avg is not None else ""
        sm_s = ("%.0f" % (acc.get("sm_wsum", 0.0) / sm_w)) if sm_w else ""
        peak_gib = acc.get("mem_peak_mib", 0) / 1024.0
        peak_s = "%.1f" % peak_gib if acc.get("mem_peak_mib") else ""
        name = render.clip(str(owner), ow).ljust(ow)
        node_val = " %-7s" % _owner_node_label(owner, owner_nodes) \
            if owner_nodes is not None else ""
        rows.append([
            ("%-3s " % ("%d." % rank), "dim"),
            (name, render.owner_tag(owner)),
            ("%s %7s %7s %8s %9s"
             % (node_val, "%.1f" % gpu_h, eff_s, sm_s, peak_s), "plain"),
        ])
    return header, rows


def _stats_columns(mode, merged_owners, daily, window_days, tight):
    """Dispatch to the right per-axis column builder. Returns (columns, ticks,
    axis_label)."""
    if mode == "hours":
        cols, ticks = hour_columns(merged_owners)
    elif mode == "days":
        cols, ticks = day_columns(daily, window_days)
    elif mode == "weeks":
        cols, ticks = week_columns(daily, window_days)
    else:  # months
        cols, ticks = month_columns(daily, window_days, tight=tight)
    return cols, ticks, STATS_MODE_LABEL[mode]


def draw_stats(stdscr, curses, stats_fetcher, statsview, painter, attrs,
               width, height):
    """Render the interactive STATS screen. Never raises on odd data."""
    days = statsview.days()
    data, error, fetched_at = stats_fetcher.get(days, statsview.scope)
    loading = stats_fetcher.loading(days, statsview.scope)

    stdscr.erase()
    y = 0
    uok = unicode_ok()

    def put(segs):
        nonlocal y
        if y >= height - 1:
            return False
        draw_segments(stdscr, curses, y, segs, attrs, width)
        y += 1
        return True

    def rule():
        put(render.divider(width, uok))

    axis_label = STATS_MODE_LABEL[statsview.mode]
    if data is None:
        put([("SGPU stats", "title"),
             ("  axis: %s" % axis_label, "dim")])
        if loading:
            put([("(loading…)", "dim")])
        elif error:
            put([(error, "warn")])
        else:
            put([("(no stats yet)", "dim")])
        _stats_footer(stdscr, curses, attrs, width, height, statsview)
        stdscr.refresh()
        return

    merged = data.get("merged") or {}
    owners = merged.get("owners") or {}
    window_days = data.get("window_days", days)
    daily = data.get("daily") or []
    awards = data.get("awards") or []
    owner_nodes = data.get("owner_nodes") if data.get("scope") == "lab" \
        else None

    age = time.time() - fetched_at if fetched_at else 0.0
    scope_label = " — all nodes" if statsview.scope == "lab" else ""
    title = [("SGPU stats — last %d days — axis: %s%s"
              % (window_days, axis_label, scope_label), "title"),
             ("  (age %ds)" % int(age), "dim")]
    if loading:
        title.append(("  (loading…)", "dim"))
    put(title)
    rule()

    # --- awards (dropped first when the terminal is short) ---
    # Reserve rows for footer(1) + grid essentials so we know our budget.
    show_awards = awards and height >= 16
    if show_awards:
        for aw in awards[:5]:
            icon = aw.get("icon") or aw.get("ascii") or "*"
            seg = [("%s " % icon, "plain"),
                   ("%s " % (aw.get("title") or ""), "header"),
                   ("%s" % (aw.get("owner") or ""),
                    render.owner_tag(aw.get("owner"))),
                   ("  %s" % (aw.get("detail") or ""), "dim")]
            put(seg)
        rule()

    # --- leaderboard (rows drop before the grid when space is tight) ---
    lb_header, lb_rows = _leaderboard_rows(owners, width, owner_nodes)
    show_leaderboard = height >= 12
    if show_leaderboard:
        put([("Leaderboard", "section")])
        put(lb_header)
        # Cap leaderboard rows so the grid always gets room.
        remaining = max(0, (height - 1) - y)
        grid_reserve = 6  # header + total + ticks + legend + a couple rows
        lb_budget = max(0, remaining - grid_reserve)
        for row in lb_rows[:lb_budget]:
            put(row)
        rule()

    # --- the GRID ---
    tight = width < 60
    columns, ticks, _axis = _stats_columns(
        statsview.mode, owners, daily, window_days, tight)

    # owner column label width for grid rows
    ow = 8
    if owners:
        ow = max(6, min(16, max(len(str(o)) for o in owners)))
    label_w = ow + 1  # one space gap before cells
    cells_area = max(2, width - label_w)
    max_cols = cells_area // 2

    trimmed = False
    if len(columns) > max_cols:
        columns = columns[-max_cols:]
        ticks = ticks[-max_cols:]
        trimmed = True

    # How many owner rows fit? Reserve: header tick line + TOTAL + legend + 1.
    grid_top = y
    rows_avail = max(1, (height - 1) - grid_top - 3)
    owner_names = top_owners_by_total(owners)
    total_owner_rows = len(owner_names)
    statsview.scroll(0, total_owner_rows, rows_avail)  # clamp offset
    start = statsview.offset
    visible_owner_names = owner_names[start:start + rows_avail]

    prefix = "…" if trimmed else ""

    # Tick / header line for the columns.
    tick_str = prefix + "".join(ticks)
    put([((" " * label_w) + tick_str, "dim")])

    def draw_grid_row(label, values, tag):
        nonlocal y
        if y >= height - 1:
            return
        row_max = max(values) if values else 0.0
        levels = [bucketize(v, row_max) for v in values]
        lbl = render.clip(str(label), ow).ljust(ow)
        draw_segments(stdscr, curses, y, [(lbl + " ", tag)], attrs, width)
        x = label_w
        if prefix:
            try:
                stdscr.addstr(y, x, "…", attrs.get("dim", 0))
            except curses.error:
                pass
            x += 1
        draw_cells(stdscr, curses, y, x, levels, painter, width)
        y += 1

    for owner in visible_owner_names:
        vals = owner_series_from_columns(columns, owner)
        draw_grid_row(owner, vals, render.owner_tag(owner))

    if start + rows_avail < total_owner_rows and y < height - 1:
        draw_segments(stdscr, curses, y,
                      [("… %d more owners (j/k)"
                        % (total_owner_rows - start - rows_avail), "dim")],
                      attrs, width)
        y += 1

    total_vals = total_series_from_columns(columns)
    draw_grid_row("TOTAL", total_vals, "header")

    # --- legend ---
    if y < height - 1:
        draw_segments(stdscr, curses, y, [("less ", "dim")], attrs, width)
        x = len("less ")
        x = draw_cells(stdscr, curses, y, x, [1, 2, 3, 4, 5], painter, width)
        try:
            stdscr.addstr(y, x, " more", attrs.get("dim", 0))
        except curses.error:
            pass
        y += 1

    _stats_footer(stdscr, curses, attrs, width, height, statsview)
    stdscr.refresh()


def _stats_footer(stdscr, curses, attrs, width, height, statsview):
    footer = ("t dashboard  a axis:%s  h/d/w/m  n scope:%s  j/k scroll  "
              "r refresh  ? help  q quit"
              % (STATS_MODE_LABEL[statsview.mode],
                 "LAB" if statsview.scope == "lab" else "LOCAL"))
    draw_segments(stdscr, curses, height - 1,
                  [(footer[:width - 1], "dim")], attrs, width)


def help_lines():
    """Build the help-overlay content as a list of (text, tag) lines.

    Uses the same style tags the rest of the TUI renders with so the overlay
    picks up colors from the shared attrs map. Kept curses-free (pure data) so
    the same builder could be unit-tested.
    """
    lines = []
    lines.append(("SGPU help", "title"))
    lines.append(("", "plain"))

    lines.append(("Keys", "section"))
    lines.append(("  dashboard: j/k,arrows scroll · Tab switch pane "
                  "(processes/pods) · s sort · o owner filter · p pause · "
                  "r refresh · t stats screen · ? help · q quit", "plain"))
    lines.append(("  stats screen: a or h/d/w/m axis (hour/day/week/month) · "
                  "n scope local/lab (all nodes) · "
                  "j/k scroll owners · r refresh · t back to dashboard · "
                  "? help · q quit", "plain"))
    lines.append(("", "plain"))

    lines.append(("Metrics", "section"))
    metrics = [
        ("UTIL", "whole-GPU utilization %: share of time the GPU was doing "
         "any work (NVML/nvidia-smi)."),
        ("SM%", "per-process SM (streaming-multiprocessor) activity: how hard "
         "that process drove the GPU cores."),
        ("MEM / PEAK-MEM", "GPU memory in use / highest seen "
         "(each H200 has ~140 GiB)."),
        ("GPU-H", "GPU-hours: time integrated over how many GPUs an owner had "
         "processes on."),
        ("EFF-H", "effective GPU-hours = GPU-H x average utilization "
         "(compute actually done)."),
        ("ALLOC-H", "allocated GPU-hours from pods' GPU requests."),
        ("IDLE-H / IDLE%", "allocated but no process running "
         "(a wasted reservation)."),
        ("REQ / ACT (pods table)", "GPUs a pod requested vs. actively "
         "using now."),
        ("POWER / TEMP", "power draw / cap, and temperature."),
        ("STORAGE", "shared pv-01/pv-02 volume usage (used/total/free)."),
    ]
    for name, desc in metrics:
        lines.append([("  %-22s " % name, "header"), (desc, "plain")])
    lines.append(("", "plain"))

    lines.append(("Awards", "section"))
    lines.append(("  owner can hold at most 3; each needs its threshold met",
                  "dim"))
    awards = [
        ("Best researcher", "most EFFECTIVE GPU-hours (GPU-H x avg util); "
         "needs >=40% avg util and >=1 GPU-H."),
        ("Power user", "most GPU-hours (>=1 GPU-H)."),
        ("Sharpshooter", "highest average SM% (>=2 GPU-H)."),
        ("Memory heavyweight", "highest peak GPU memory (>=32 GiB)."),
        ("Night owl", "biggest share of own activity in KST 00-05h "
         "(>=1 GPU-H in window)."),
        ("Most headroom", "lowest avg util among heavy users "
         "(>=4 GPU-H and util <40%): free speedup waiting."),
        ("Seat warmer", "most idle allocated GPU-hours (needs the "
         "pod-allocation view; >=2 idle GPU-H)."),
    ]
    for name, desc in awards:
        lines.append([("  %s " % name, "header"), (desc, "plain")])
    lines.append(("", "plain"))

    lines.append(("↑/↓/j/k scroll · any other key closes help", "dim"))
    return lines


def clamp_help_offset(offset, line_count, visible_rows):
    """Clamp a help scroll offset into [0, max(0, line_count - visible_rows)]."""
    max_offset = max(0, line_count - max(0, visible_rows))
    if offset < 0:
        return 0
    if offset > max_offset:
        return max_offset
    return offset


def draw_help(stdscr, curses, attrs, width, height, offset):
    """Render the scrollable help overlay. Returns the clamped offset used.

    Draws `help_lines()` starting at row 0, skipping `offset` lines. Each line
    may be a single (text, tag) tuple or a list of segments; both are handed to
    draw_segments, which clips to width. Never raises on a short terminal.
    """
    lines = help_lines()
    visible_rows = max(0, height)
    offset = clamp_help_offset(offset, len(lines), visible_rows)
    # clear() (vs erase()) forces a full repaint on the next refresh so the
    # underlying dashboard/stats cells never bleed through the overlay.
    stdscr.clear()
    y = 0
    for line in lines[offset:offset + visible_rows]:
        if y >= height:
            break
        segs = line if isinstance(line, list) else [line]
        draw_segments(stdscr, curses, y, segs, attrs, width)
        y += 1
    stdscr.refresh()
    return offset


def run_tui(stdscr, curses, fetcher, view):
    curses.curs_set(0)
    stdscr.timeout(200)
    try:
        curses.mousemask(curses.BUTTON4_PRESSED | curses.BUTTON5_PRESSED)
    except (curses.error, AttributeError):
        pass
    attrs = build_tag_attrs(curses)
    bars_unicode = unicode_ok()
    painter = GridPainter(curses, bars_unicode)
    stats_fetcher = StatsFetcher()
    stats_fetcher.start()
    statsview = StatsView()
    last_generation = -1
    last_size = stdscr.getmaxyx()
    shown = None
    dirty = True
    help_open = False
    help_offset = 0
    help_drawn = False

    while True:
        key = stdscr.getch()

        # --- help overlay: renders over whichever screen is active and routes
        # keys to itself, leaving the underlying screen state untouched so
        # closing restores exactly where the user was. ---
        if help_open:
            size = stdscr.getmaxyx()
            if size != last_size:
                last_size = size
                try:
                    curses.resizeterm(*size)
                except curses.error:
                    pass
                help_drawn = False
            height, width = size
            visible_rows = max(1, height)
            if key != -1:
                if key in (ord("j"), curses.KEY_DOWN):
                    help_offset += 1
                elif key in (ord("k"), curses.KEY_UP):
                    help_offset -= 1
                elif key == curses.KEY_NPAGE:
                    help_offset += max(1, visible_rows - 1)
                elif key == curses.KEY_PPAGE:
                    help_offset -= max(1, visible_rows - 1)
                elif key == ord("g"):
                    help_offset = 0
                elif key == ord("G"):
                    help_offset = len(help_lines())
                else:
                    # any other key (incl. ?, q, Esc) closes help and returns
                    # to the previous screen without quitting the app.
                    help_open = False
                    help_offset = 0
                    dirty = True
                    continue
                help_drawn = False  # a scroll key moved the view
            # The help text is static; only repaint on open/scroll/resize so
            # the per-tick loop doesn't clear() the screen and flicker.
            if not help_drawn:
                help_offset = draw_help(stdscr, curses, attrs, width, height,
                                        help_offset)
                help_drawn = True
            continue

        # --- STATS screen: independent of the live snapshot ---
        if view.screen == "stats":
            size = stdscr.getmaxyx()
            if size != last_size:
                last_size = size
                try:
                    curses.resizeterm(*size)
                except curses.error:
                    pass
                dirty = True
            height, width = size
            # lazily fetch the current mode's window on entry / mode switch
            stats_fetcher.request(statsview.days(), statsview.scope)
            data, _serr, _sat = stats_fetcher.get(statsview.days())
            owners = ((data or {}).get("merged") or {}).get("owners") or {}
            n_owners = len(owners)
            if key != -1:
                dirty = True
                if key in (ord("q"), 3):
                    return
                elif key in (ord("t"), 27):
                    view.screen = "dash"
                    continue
                elif key == ord("?"):
                    help_open = True
                    help_offset = 0
                    help_drawn = False
                    continue
                elif key == ord("a"):
                    statsview.cycle()
                    stats_fetcher.request(statsview.days(), statsview.scope)
                elif key == ord("h"):
                    statsview.set_mode("hours")
                    stats_fetcher.request(statsview.days(), statsview.scope)
                elif key == ord("d"):
                    statsview.set_mode("days")
                    stats_fetcher.request(statsview.days(), statsview.scope)
                elif key == ord("w"):
                    statsview.set_mode("weeks")
                    stats_fetcher.request(statsview.days(), statsview.scope)
                elif key == ord("m"):
                    statsview.set_mode("months")
                    stats_fetcher.request(statsview.days(), statsview.scope)
                elif key == ord("n"):
                    statsview.toggle_scope()
                    stats_fetcher.request(statsview.days(), statsview.scope)
                elif key == ord("r"):
                    stats_fetcher.request(statsview.days(), statsview.scope, force=True)
                elif key in (ord("j"), curses.KEY_DOWN):
                    statsview.scroll(1, n_owners, max(1, height - 8))
                elif key in (ord("k"), curses.KEY_UP):
                    statsview.scroll(-1, n_owners, max(1, height - 8))
                elif key == curses.KEY_NPAGE:
                    statsview.scroll(5, n_owners, max(1, height - 8))
                elif key == curses.KEY_PPAGE:
                    statsview.scroll(-5, n_owners, max(1, height - 8))
                elif key == curses.KEY_MOUSE:
                    try:
                        _, _, _, _, bstate = curses.getmouse()
                        if bstate & curses.BUTTON4_PRESSED:
                            statsview.scroll(-3, n_owners, max(1, height - 8))
                        elif bstate & curses.BUTTON5_PRESSED:
                            statsview.scroll(3, n_owners, max(1, height - 8))
                    except curses.error:
                        pass
            # Redraw every tick (loading state / age counter advance) or on key.
            draw_stats(stdscr, curses, stats_fetcher, statsview, painter,
                       attrs, width, height)
            continue

        snapshot, error, fetched_at, generation = fetcher.state()
        if snapshot is None:
            stdscr.erase()
            draw_segments(stdscr, curses, 0,
                          [(error or "connecting to sgpu server...", "warn")],
                          attrs, last_size[1])
            stdscr.refresh()
            if key in (ord("q"), 3):
                return
            continue
        if not view.paused and generation != last_generation:
            shown = snapshot
            last_generation = generation
            dirty = True
        if shown is None:
            shown = snapshot

        size = stdscr.getmaxyx()
        if size != last_size:
            last_size = size
            try:
                curses.resizeterm(*size)
            except curses.error:
                pass
            dirty = True
        height, width = size

        procs = view.visible_procs(shown)
        pods = view.visible_pods(shown)

        if key != -1:
            dirty = True
            if key in (ord("q"), 3, 27):
                return
            elif key == ord("t"):
                view.screen = "stats"
                stats_fetcher.request(statsview.days(), statsview.scope)
                continue
            elif key == ord("?"):
                help_open = True
                help_offset = 0
                help_drawn = False
                continue
            elif key in (ord("j"), curses.KEY_DOWN):
                view.move(1, len(procs) if view.focus == "procs" else len(pods))
            elif key in (ord("k"), curses.KEY_UP):
                view.move(-1, len(procs) if view.focus == "procs" else len(pods))
            elif key == curses.KEY_NPAGE:
                view.move(10, len(procs) if view.focus == "procs" else len(pods))
            elif key == curses.KEY_PPAGE:
                view.move(-10, len(procs) if view.focus == "procs" else len(pods))
            elif key == ord("g"):
                view.selected[view.focus] = 0
            elif key == ord("G"):
                count = len(procs) if view.focus == "procs" else len(pods)
                view.selected[view.focus] = max(0, count - 1)
            elif key == ord("\t"):
                view.focus = "pods" if view.focus == "procs" else "procs"
            elif key == ord("s"):
                view.sort = SORT_MODES[
                    (SORT_MODES.index(view.sort) + 1) % len(SORT_MODES)]
            elif key == ord("o"):
                view.cycle_owner(shown)
            elif key == ord("p"):
                view.paused = not view.paused
            elif key == ord("r"):
                fetcher.poke()
            elif key == curses.KEY_MOUSE:
                try:
                    _, _, _, _, bstate = curses.getmouse()
                    if bstate & curses.BUTTON4_PRESSED:
                        view.move(-3, len(procs) if view.focus == "procs"
                                  else len(pods))
                    elif bstate & curses.BUTTON5_PRESSED:
                        view.move(3, len(procs) if view.focus == "procs"
                                  else len(pods))
                except curses.error:
                    pass
            elif key == curses.KEY_RESIZE:
                pass  # size handled above

        if not dirty:
            continue
        dirty = False

        filtered = dict(shown, procs=procs)
        banner_segs = update_banner_segments(shown)
        header_lines = render.layout_header(shown, width)
        gpu_lines = render.layout_gpus(shown, width, bars_unicode)
        free_lines = render.layout_free_summary(shown, width)
        storage_lines = render.layout_storage(shown, width, bars_unicode)
        proc_layout = render.layout_procs(filtered, width)
        pod_layout = render.layout_pods(dict(shown, pods=dict(
            shown.get("pods") or {}, rows=pods)), width)
        div = render.divider(width, bars_unicode)

        # Vertical budget: [banner] + header + divider + gpus + free + storage
        # + divider + procs header + procs rows (flex) + [divider + pods] +
        # footer. Section dividers replace the old blank separators.
        fixed_top = (1 if banner_segs else 0) + len(header_lines) + 1 \
            + len(gpu_lines) + len(free_lines) + len(storage_lines) + 1 \
            + len(proc_layout["header"])
        pods_block = 1 + len(pod_layout["header"]) \
            + min(len(pod_layout["rows"]), 6)
        if height < 24:
            pods_block = 0
        proc_view = max(1, height - fixed_top - pods_block - 1)
        view.clamp("procs", len(procs), proc_view)
        pods_view = max(0, pods_block - 1 - len(pod_layout["header"])) \
            if pods_block else 0
        view.clamp("pods", len(pods), max(1, pods_view) if pods_block else 1)

        stdscr.erase()
        y = 0
        if banner_segs:
            draw_segments(stdscr, curses, y, banner_segs, attrs, width)
            y += 1
        for line in header_lines:
            age = time.time() - fetched_at
            status = "  %s%s" % ("PAUSED  " if view.paused else "",
                                 "(%0.1fs)" % age)
            draw_segments(stdscr, curses, y,
                          line + [(status, "dim")]
                          + ([(" " + error, "crit")] if error else []),
                          attrs, width)
            y += 1
        draw_segments(stdscr, curses, y, div, attrs, width)
        y += 1
        for line in gpu_lines:
            draw_segments(stdscr, curses, y, line, attrs, width)
            y += 1
        for line in free_lines:
            draw_segments(stdscr, curses, y, line, attrs, width)
            y += 1
        for line in storage_lines:
            draw_segments(stdscr, curses, y, line, attrs, width)
            y += 1
        draw_segments(stdscr, curses, y, div, attrs, width)
        y += 1
        for line in proc_layout["header"]:
            draw_segments(stdscr, curses, y, line, attrs, width)
            y += 1
        proc_rows = proc_layout["rows"]
        start = view.offset["procs"]
        for row_index in range(start, min(len(proc_rows), start + proc_view)):
            draw_segments(stdscr, curses, y, proc_rows[row_index], attrs,
                          width,
                          reverse=(view.focus == "procs"
                                   and row_index == view.selected["procs"]
                                   and len(procs) > 0))
            y += 1
        if len(proc_rows) > start + proc_view:
            draw_segments(stdscr, curses, y - 1,
                          [("... %d more (scroll)" %
                            (len(proc_rows) - start - proc_view + 1), "dim")],
                          attrs, width)
        if pods_block:
            draw_segments(stdscr, curses, y, div, attrs, width)
            y += 1
            for line in pod_layout["header"]:
                draw_segments(stdscr, curses, y, line, attrs, width)
                y += 1
            pod_rows = pod_layout["rows"]
            pod_start = view.offset["pods"]
            for row_index in range(pod_start,
                                   min(len(pod_rows), pod_start + pods_view)):
                draw_segments(stdscr, curses, y, pod_rows[row_index], attrs,
                              width,
                              reverse=(view.focus == "pods"
                                       and row_index == view.selected["pods"]
                                       and len(pods) > 0))
                y += 1
        footer = ("q quit  ? help  t stats  j/k scroll  Tab pane:%s  "
                  "s sort:%s  o owner:%s  p pause  r refresh"
                  % (view.focus, view.sort, view.owner_filter or "all"))
        draw_segments(stdscr, curses, height - 1,
                      [(footer[:width - 1], "dim")], attrs, width)
        stdscr.refresh()


def main():
    interval = DEFAULT_INTERVAL
    for arg in sys.argv[1:]:
        try:
            interval = max(0.5, float(arg))
        except ValueError:
            print("usage: tui.py [interval-seconds]", file=sys.stderr)
            return 2
    try:
        locale.setlocale(locale.LC_ALL, "")
    except locale.Error:
        pass

    fetcher = Fetcher(interval)
    fetcher.start()

    if not sys.stdout.isatty():
        snapshot, error = fetcher.fetch_once()
        if snapshot is None:
            print(error, file=sys.stderr)
            return 1
        print(render.render_text(snapshot, color=False), end="")
        return 0

    view = View()
    try:
        import curses
        curses.wrapper(lambda stdscr: run_tui(stdscr, curses, fetcher, view))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        # curses failed (weird TERM etc.) — degrade to one-shot text.
        snapshot, error = fetcher.fetch_once()
        if snapshot is not None:
            print(render.render_text(snapshot, color=False), end="")
            print("(tui unavailable: %s)" % exc, file=sys.stderr)
            return 0
        print("sgpu tui failed: %s" % exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
