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

os.environ.setdefault("TERM", "xterm-256color")

import render

JSON_URL = os.environ.get("SGPU_JSON_URL", "http://127.0.0.1:8080/json")
DEFAULT_INTERVAL = float(os.environ.get("SGPU_TUI_INTERVAL", "2"))

SORT_MODES = ("gpu", "mem", "owner")


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


def run_tui(stdscr, curses, fetcher, view):
    curses.curs_set(0)
    stdscr.timeout(200)
    try:
        curses.mousemask(curses.BUTTON4_PRESSED | curses.BUTTON5_PRESSED)
    except (curses.error, AttributeError):
        pass
    attrs = build_tag_attrs(curses)
    bars_unicode = unicode_ok()
    last_generation = -1
    last_size = stdscr.getmaxyx()
    shown = None
    dirty = True

    while True:
        key = stdscr.getch()
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
        header_lines = render.layout_header(shown, width)
        gpu_lines = render.layout_gpus(shown, width, bars_unicode)
        storage_lines = render.layout_storage(shown, width, bars_unicode)
        proc_layout = render.layout_procs(filtered, width)
        pod_layout = render.layout_pods(dict(shown, pods=dict(
            shown.get("pods") or {}, rows=pods)), width)

        # Vertical budget: header 1 + blank + gpus + blank + procs header 2
        # + procs rows (flex) + blank + pods (<=8, collapses first) + footer.
        fixed_top = len(header_lines) + 1 + len(gpu_lines) \
            + len(storage_lines) + 1 + len(proc_layout["header"])
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
        for line in header_lines:
            age = time.time() - fetched_at
            status = "  %s%s" % ("PAUSED  " if view.paused else "",
                                 "(%0.1fs)" % age)
            draw_segments(stdscr, curses, y,
                          line + [(status, "dim")]
                          + ([(" " + error, "crit")] if error else []),
                          attrs, width)
            y += 1
        y += 1
        for line in gpu_lines:
            draw_segments(stdscr, curses, y, line, attrs, width)
            y += 1
        for line in storage_lines:
            draw_segments(stdscr, curses, y, line, attrs, width)
            y += 1
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
        footer = ("q quit  j/k scroll  PgUp/Dn  g/G  Tab pane:%s  "
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
