"""Tests for the TUI activity spinners (pure, curses-free helpers)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools",
                                "gpu-monitor"))

import tui       # noqa: E402
import render    # noqa: E402


class TestNodeTargetsAndFooters(unittest.TestCase):
    def test_node_targets_put_current_namespace_first(self):
        targets = tui.node_targets(
            namespace="p-sgvr-node-02",
            peers="node-01=http://n1:8080,node-02=http://n2:8080")
        self.assertEqual([row[0] for row in targets], ["node-02", "node-01"])
        self.assertEqual(targets[1][1], "http://n1:8080/json")
        self.assertEqual(targets[1][2], "http://n1:8080/stats")

    def test_stats_scope_cycles_current_other_lab(self):
        view = tui.StatsView(["node-02", "node-01"])
        self.assertEqual(view.scope_label(), "2")
        view.toggle_scope()
        self.assertEqual(view.scope_label(), "1")
        view.toggle_scope()
        self.assertEqual(view.scope_label(), "LAB")

    def test_footer_drops_whole_chunks_instead_of_clipping_words(self):
        text = tui.footer_text(
            ["q quit", "? help", "Enter detail", "r refresh"], width=34)
        self.assertLessEqual(len(text), 33)
        self.assertNotIn("refres", text)

    def test_help_wraps_and_matches_node_scope_labels(self):
        lines = tui.help_lines(72)
        text = "\n".join("".join(seg for seg, _tag in (
            line if isinstance(line, list) else [line]))
                         for line in lines)
        self.assertIn("n scope 1/2/LAB", text)
        self.assertNotIn("current/other/LAB", text)
        self.assertGreaterEqual(
            sum(1 for line in lines
                if isinstance(line, list) and line and line[0][1] == "rule"),
            3)
        for line in lines:
            plain = "".join(seg for seg, _tag in (
                line if isinstance(line, list) else [line]))
            self.assertLessEqual(len(plain), 72)


class TestGpuSpinner(unittest.TestCase):
    def test_idle_is_static_dot(self):
        for util in (None, 0, 3):
            self.assertEqual(tui.gpu_spinner_glyph(util, 12345), ".")

    def test_active_glyph_from_braille_set(self):
        g = tui.gpu_spinner_glyph(80, 5, unicode_ok=True)
        self.assertIn(g, tui.GPU_SPIN)

    def test_ascii_fallback(self):
        g = tui.gpu_spinner_glyph(80, 5, unicode_ok=False)
        self.assertIn(g, tui.GPU_SPIN_ASCII)

    def test_higher_util_advances_faster(self):
        # Over a short span (before the slow one completes a cycle), a busier
        # GPU steps through more distinct spinner frames (spins faster).
        def distinct(util):
            return len({tui.gpu_spinner_glyph(util, f) for f in range(20)})
        self.assertGreater(distinct(100), distinct(20))

    def test_animates_over_frames(self):
        # A fixed active util still changes glyph as the frame advances.
        seen = {tui.gpu_spinner_glyph(90, f) for f in range(0, 200, 1)}
        self.assertGreater(len(seen), 1)


class TestStatsSpinner(unittest.TestCase):
    def test_cycles(self):
        seen = {tui.stats_loading_glyph(f) for f in range(0, 60)}
        self.assertGreater(len(seen), 1)
        for f in range(60):
            self.assertIn(tui.stats_loading_glyph(f), tui.STATS_SPIN)

    def test_ascii(self):
        self.assertIn(tui.stats_loading_glyph(3, unicode_ok=False),
                      tui.STATS_SPIN_ASCII)


class TestStatsPulse(unittest.TestCase):
    def _insights(self):
        return {
            "has_device_telemetry": True,
            "util_avg": 57.0,
            "vram_avg": 41.0,
            "util_by_hour_kst": [0, 20, 45, 90] * 6,
            "util_bands": {"hot": 35.0},
            "busy_windows": 4,
            "telemetry_dates_covered": ["20260706"],
            "available_dates_covered": ["20260701", "20260702",
                                        "20260703", "20260704",
                                        "20260705", "20260706",
                                        "20260707"],
        }

    def test_wide_pulse_keeps_spark_and_partial_coverage(self):
        text = tui.format_pulse_summary(self._insights(), width=100,
                                        unicode_ok=True)
        self.assertIn("PULSE KST", text)
        self.assertIn("57%", text)
        self.assertIn("partial 1/7d", text)
        self.assertTrue(any(glyph in text for glyph in "▁▂▃▄▅▆▇█"))

    def test_ascii_and_narrow_pulse_are_safe(self):
        ascii_text = tui.format_pulse_summary(self._insights(), width=70,
                                              unicode_ok=False)
        self.assertIn("PULSE", ascii_text)
        self.assertNotIn("▁", ascii_text)
        narrow = tui.format_pulse_summary(self._insights(), width=45,
                                          unicode_ok=False)
        self.assertIn("PULSE util 57%", narrow)

    def test_lab_partial_coverage_uses_node_days(self):
        insights = self._insights()
        insights["telemetry_coverage"] = {
            "covered": 7, "available": 14, "unit": "node-days"}
        insights["has_flow"] = True
        insights["flow_scope"] = "node"
        text = tui.format_pulse_summary(insights, width=100,
                                        unicode_ok=False)
        self.assertIn("partial 7/14 node-days", text)
        self.assertIn("4 node windows", text)

    def test_old_rollup_has_no_pulse_line(self):
        self.assertIsNone(tui.format_pulse_summary({
            "has_device_telemetry": False}, width=100))

    def test_momentum_prefers_current_streak(self):
        text = tui.format_momentum_summary([
            {"owner": "youngju", "observed_days": 7, "active_days": 6,
             "current_streak_days": 4},
        ], width=100)
        self.assertIn("MOMENTUM youngju", text)
        self.assertIn("4d streak", text)


class TestNonblockingInputPacing(unittest.TestCase):
    def test_frame_sleep_seconds_until_next_frame(self):
        self.assertAlmostEqual(
            tui.frame_sleep_seconds(10.0, 10.0), tui._FRAME_INTERVAL)
        self.assertGreater(tui.frame_sleep_seconds(
            10.01, 10.0), 0.0)
        self.assertEqual(tui.frame_sleep_seconds(
            10.0 + tui._FRAME_INTERVAL, 10.0), 0.0)

    def test_poll_key_does_not_call_getch_without_ready_input(self):
        class Screen:
            def getch(self):
                raise AssertionError("getch must not run without readable input")

        self.assertEqual(
            tui.poll_key(Screen(), select_fn=lambda *_: ([], [], [])), -1)

    def test_poll_key_reads_when_input_is_ready(self):
        class Screen:
            def getch(self):
                return ord("r")

        self.assertEqual(
            tui.poll_key(Screen(), select_fn=lambda *_: ([0], [], [])),
            ord("r"))


class TestNonblockingOutput(unittest.TestCase):
    class FakeCurses:
        error = RuntimeError

    class Screen:
        def __init__(self):
            self.calls = 0
            self.touches = 0
            self.blocked = True

        def refresh(self):
            self.calls += 1
            if self.blocked:
                raise BlockingIOError("PTY is full")

        def touchwin(self):
            self.touches += 1

    class StalledOutput:
        broken = False

        def flush_pending(self):
            return False

        def stalled(self, now, timeout):
            return True

    def test_refresh_backpressure_is_dropped_and_retried(self):
        now = [10.0]
        screen = self.Screen()
        refresher = tui.RefreshController(
            clock=lambda: now[0], min_backoff=0.1, max_backoff=0.2)

        self.assertFalse(refresher.refresh(screen, self.FakeCurses()))
        self.assertEqual((screen.calls, screen.touches), (1, 1))

        now[0] = 10.05
        self.assertFalse(refresher.refresh(screen, self.FakeCurses()))
        self.assertEqual(screen.calls, 1)  # bounded: no busy retry

        now[0] = 10.1
        screen.blocked = False
        self.assertTrue(refresher.refresh(screen, self.FakeCurses()))
        self.assertEqual(refresher.failures, 0)

    def test_output_watchdog_exits_after_sustained_no_progress(self):
        refresher = tui.RefreshController(
            clock=lambda: 10.0, output=self.StalledOutput(),
            output_stall_seconds=5)
        with self.assertRaises(tui.OutputDisconnected):
            refresher.refresh(self.Screen(), self.FakeCurses())

    def test_output_descriptor_is_restored(self):
        read_fd, write_fd = os.pipe()
        try:
            with os.fdopen(write_fd, "wb", closefd=False) as stream:
                output = tui.CursesOutputBuffer(stream)
                os.write(write_fd, b"frame")
                self.assertEqual(output.take_frame(), b"frame")
                self.assertTrue(output.send_frame(b"delivered"))
                self.assertEqual(os.read(read_fd, 64), b"delivered")
                output.close()
        finally:
            os.close(read_fd)
            os.close(write_fd)

    def test_pending_output_tolerates_brief_backpressure_then_marks_stalled(self):
        now = [0.0]
        read_fd, write_fd = os.pipe()
        stream = os.fdopen(write_fd, "wb", closefd=True)
        output = None
        try:
            output = tui.CursesOutputBuffer(stream, clock=lambda: now[0])
            while True:
                try:
                    os.write(output.real_fd, b"x" * 65536)
                except BlockingIOError:
                    break
            self.assertFalse(output.send_frame(b"frame"))
            self.assertFalse(output.stalled(4.9, 5))
            self.assertTrue(output.stalled(5.0, 5))
            # close() must not wait for the permanently full consumer.
            output.close()
            output = None
        finally:
            if output is not None:
                output.close()
            stream.close()
            os.close(read_fd)

    @unittest.skipUnless(os.name == "posix" and hasattr(os, "fork"),
                         "requires a POSIX pseudo-terminal")
    def test_real_curses_loop_exits_when_unread_pty_never_recovers(self):
        """A disconnected exec PTY must not leave an orphan TUI process.

        The child paints changing full screens into a PTY whose master is
        intentionally never read. A short output stall is harmless, but once
        no byte reaches the PTY for the watchdog window the child exits. The
        parent waits for that exact child, making orphan-process regressions
        deterministic.
        """
        import curses
        import fcntl
        import select
        import signal
        import struct
        import termios
        import time

        master_fd, slave_fd = os.openpty()
        heartbeat_read, heartbeat_write = os.pipe()
        fcntl.ioctl(slave_fd, termios.TIOCSWINSZ,
                    struct.pack("HHHH", 24, 80, 0, 0))
        child = os.fork()
        if child == 0:
            try:
                os.close(master_fd)
                os.close(heartbeat_read)
                for fd in (0, 1, 2):
                    os.dup2(slave_fd, fd)
                if slave_fd > 2:
                    os.close(slave_fd)

                output = tui.CursesOutputBuffer()
                disconnected = [False]

                def paint(stdscr):
                    refresher = tui.RefreshController(
                        output=output, output_stall_seconds=0.25)
                    deadline = time.monotonic() + 2.0
                    frame = 0
                    while time.monotonic() < deadline:
                        char = "A" if frame % 2 else "B"
                        stdscr.erase()
                        height, width = stdscr.getmaxyx()
                        for row in range(max(0, height - 1)):
                            try:
                                stdscr.addstr(row, 0, char * max(0, width - 1))
                            except curses.error:
                                pass
                        try:
                            refresher.refresh(stdscr, curses)
                        except tui.OutputDisconnected:
                            disconnected[0] = True
                            return
                        os.write(heartbeat_write, b".")
                        frame += 1
                        time.sleep(0.005)

                try:
                    curses.wrapper(paint)
                finally:
                    output.close()
                os._exit(0 if disconnected[0] else 3)
            except BaseException:
                os._exit(2)

        os.close(slave_fd)
        os.close(heartbeat_write)
        status = None
        beats = 0
        deadline = time.monotonic() + 4.0
        try:
            while time.monotonic() < deadline:
                ready, _, _ = select.select([heartbeat_read], [], [], 0.1)
                if ready:
                    chunk = os.read(heartbeat_read, 65536)
                    if chunk:
                        beats += len(chunk)
                waited, child_status = os.waitpid(child, os.WNOHANG)
                if waited == child:
                    status = child_status
                    break
            if status is None:
                def proc_text(name):
                    try:
                        with open("/proc/%d/%s" % (child, name)) as fh:
                            return fh.read().strip()
                    except OSError as exc:
                        return type(exc).__name__

                diagnostics = {
                    "wchan": proc_text("wchan"),
                    "syscall": proc_text("syscall"),
                    "fdinfo": proc_text("fdinfo/1"),
                }
                os.kill(child, signal.SIGKILL)
                _, status = os.waitpid(child, 0)
                self.fail("curses child stalled under PTY backpressure: %r"
                          % diagnostics)
            self.assertEqual(os.waitstatus_to_exitcode(status), 0)
            self.assertGreater(beats, 5)
        finally:
            os.close(heartbeat_read)
            os.close(master_fd)


class TestOwnerColors(unittest.TestCase):
    LAB_NAMES = [
        "sangmin", "taegun", "ty", "yoonki", "kbhan",
        "alex", "junho", "minji", "seung", "hyun",
        "jiyoon", "jaewon", "donghyun", "young", "jisoo",
        "hyejin", "taeho", "minsu", "jiho", "soyeon",
    ]

    def test_palette_exposes_twenty_owner_tags(self):
        owner_keys = [k for k in render.ANSI if k.startswith("o")
                      and k[1:].isdigit()]
        self.assertEqual(render.OWNER_TAG_COUNT, 20)
        self.assertEqual(len(owner_keys), 20)

    def test_visible_owner_map_avoids_current_screen_collisions(self):
        tags = render.owner_tag_map(self.LAB_NAMES)
        self.assertNotEqual(tags["sangmin"], tags["taegun"])
        self.assertEqual(len(set(tags.values())), len(self.LAB_NAMES))

    def test_visible_owner_map_is_order_independent(self):
        forward = render.owner_tag_map(self.LAB_NAMES)
        reverse = render.owner_tag_map(list(reversed(self.LAB_NAMES)))
        self.assertEqual(forward, reverse)

    def test_dashboard_layouts_share_owner_map(self):
        snap = {
            "gpus": [{
                "index": 0, "name": "H200", "util": 98,
                "mem_used_mib": 1000, "mem_total_mib": 143771,
                "temp_c": 60, "power_w": 500, "power_limit_w": 700,
                "owners": ["sangmin", "taegun"],
            }],
            "procs": [
                {"gpu_index": 0, "owner": "sangmin", "pod": "sangmin-a",
                 "pid": 1, "sm_util": 98, "mem_mib": 1000},
                {"gpu_index": 0, "owner": "taegun", "pod": "taegun-b",
                 "pid": 2, "sm_util": 97, "mem_mib": 1000},
            ],
            "pods": {"ok": True, "rows": [
                {"owner": "sangmin", "pod": "sangmin-a", "gpu": 1,
                 "active": 1, "age": "1m", "phase": "Running"},
                {"owner": "taegun", "pod": "taegun-b", "gpu": 1,
                 "active": 1, "age": "1m", "phase": "Running"},
            ]},
        }
        tags = render.owner_tag_map(render.snapshot_owners(snap))
        gpu_line = render.layout_gpus(
            snap, 120, True, owner_tags=tags)[1]
        procs = render.layout_procs(
            snap, 120, owner_tags=tags)["rows"]
        pods = render.layout_pods(
            snap, 120, owner_tags=tags)["rows"]

        gpu_tags = {text: tag for text, tag in gpu_line
                    if text in ("sangmin", "taegun")}
        proc_tags = {row[1][0].strip(): row[1][1] for row in procs}
        pod_tags = {row[0][0].strip(): row[0][1] for row in pods}

        for owner in ("sangmin", "taegun"):
            self.assertEqual(gpu_tags[owner], tags[owner])
            self.assertEqual(proc_tags[owner], tags[owner])
            self.assertEqual(pod_tags[owner], tags[owner])


class TestCursesAttrs(unittest.TestCase):
    class FakeCurses:
        COLORS = 256
        COLOR_GREEN = 2
        COLOR_YELLOW = 3
        COLOR_RED = 1
        COLOR_CYAN = 6
        COLOR_MAGENTA = 5
        COLOR_BLUE = 4
        COLOR_BLACK = 0
        A_BOLD = 1 << 16
        A_DIM = 1 << 17
        error = RuntimeError

        def __init__(self):
            self.pairs = []

        def has_colors(self):
            return True

        def start_color(self):
            pass

        def use_default_colors(self):
            pass

        def init_pair(self, pair_number, foreground, background):
            self.pairs.append((pair_number, foreground, background))

        def color_pair(self, pair_number):
            return pair_number << 8

    def test_build_tag_attrs_registers_all_owner_tags(self):
        fake = self.FakeCurses()
        attrs = tui.build_tag_attrs(fake)
        for i in range(render.OWNER_TAG_COUNT):
            self.assertIn("o%d" % i, attrs)
        self.assertGreaterEqual(len(fake.pairs), render.OWNER_TAG_COUNT)


class TestLayoutGutter(unittest.TestCase):
    def _snap(self):
        return {"gpus": [{"index": 0, "name": "H200", "util": 90,
                          "mem_used_mib": 1000, "mem_total_mib": 143771,
                          "temp_c": 60, "power_w": 500, "power_limit_w": 700,
                          "owners": ["a"]}]}

    def test_spinner_gutter_present(self):
        lines = render.layout_gpus(self._snap(), 120, True,
                                   spinners=[("X", "ok")])
        row = lines[1]  # first GPU data row (lines[0] is header)
        self.assertEqual(row[0], ("X", "ok"))

    def test_static_gutter_without_spinners(self):
        lines = render.layout_gpus(self._snap(), 120, True)
        # active GPU -> dim "* " gutter
        self.assertEqual(lines[1][0], ("* ", "dim"))

    def test_header_indented(self):
        lines = render.layout_gpus(self._snap(), 120, True)
        header_text = lines[0][0][0]
        self.assertTrue(header_text.startswith("  GPU"))


class TestDetailLines(unittest.TestCase):
    def _snap(self):
        return {
            "gpus": [{
                "index": 3, "uuid": "GPU-ty", "name": "NVIDIA H200",
                "util": 96, "mem_used_mib": 62458,
                "mem_total_mib": 143771, "temp_c": 68,
                "power_w": 563, "power_limit_w": 700, "owners": ["ty"],
            }],
            "procs": [{
                "pid": 2621452, "gpu_index": 3, "gpu_uuid": "GPU-ty",
                "mem_mib": 62458, "owner": "ty", "pod": "ty-lpwm-panda2t",
                "pod_uid": None, "cmd": "python train_lpwm.py --run-name x",
                "started_utc": "2026-07-05T02:45:52Z", "sm_util": 96,
                "attribution": "environ",
            }],
            "pods": {"ok": True, "rows": [{
                "owner": "ty", "pod": "ty-lpwm-panda2t",
                "node": "h200-04-w-4b11", "phase": "Running",
                "gpu": 1, "age": "3d16h", "uid": "pod-uid",
                "start_iso": "2026-07-05T02:45:47Z", "active": 1,
            }]},
        }

    def _plain(self, lines):
        return "\n".join("".join(text for text, _tag in line)
                         for line in lines)

    def test_process_detail_includes_gpu_pod_and_command(self):
        ref = {"kind": "proc", "pid": 2621452}
        text = self._plain(tui.detail_lines(self._snap(), ref, 100))
        self.assertIn("process pid=2621452", text)
        self.assertIn("ty-lpwm-panda2t", text)
        self.assertIn("GPU-ty", text)
        self.assertIn("python train_lpwm.py", text)

    def test_pod_detail_includes_active_gpu_and_related_proc(self):
        ref = {"kind": "pod", "pod": "ty-lpwm-panda2t", "uid": "pod-uid"}
        text = self._plain(tui.detail_lines(self._snap(), ref, 100))
        self.assertIn("pod ty-lpwm-panda2t", text)
        self.assertIn("Active GPUs", text)
        self.assertIn("GPU 3", text)
        self.assertIn("2621452", text)

    def test_related_process_command_wraps_instead_of_disappearing(self):
        snap = self._snap()
        snap["procs"][0]["cmd"] = (
            "python train_lpwm.py --config configs/very-long-config.yaml "
            "--output-dir /output/long/path --run-name wrapped-command")
        ref = {"kind": "pod", "pod": "ty-lpwm-panda2t", "uid": "pod-uid"}
        text = self._plain(tui.detail_lines(snap, ref, 68))
        self.assertIn("--output-dir", text)
        self.assertIn("wrapped-command", text)

    def test_related_process_wrapped_command_keeps_same_color(self):
        snap = self._snap()
        snap["procs"][0]["cmd"] = (
            "python train_lpwm.py --config configs/very-long-config.yaml "
            "--output-dir /output/long/path --run-name wrapped-command")
        ref = {"kind": "pod", "pod": "ty-lpwm-panda2t", "uid": "pod-uid"}
        lines = tui.detail_lines(snap, ref, 68)
        wrapped_tags = []
        for line in lines:
            for text, tag in line:
                if "--output-dir" in text or "wrapped-command" in text:
                    wrapped_tags.append(tag)
        self.assertTrue(wrapped_tags)
        self.assertEqual(set(wrapped_tags), {"crit"})

    def test_stale_process_uses_last_known_values(self):
        ref = {"kind": "proc", "pid": 7,
               "snapshot": {"pid": 7, "owner": "old", "pod": "old-pod"}}
        text = self._plain(tui.detail_lines({"procs": []}, ref, 80))
        self.assertIn("stale", text)
        self.assertIn("old-pod", text)


class TestDashboardRowBudget(unittest.TestCase):
    def test_tall_screen_expands_pods_until_all_visible(self):
        proc_view, pods_view, pods_block = tui.dashboard_row_budget(
            height=39, fixed_top=16, proc_rows=10, pod_rows=8,
            pod_header_rows=2)
        self.assertGreaterEqual(proc_view, 10)
        self.assertEqual(pods_view, 8)
        self.assertEqual(pods_block, 11)

    def test_shorter_screen_keeps_pod_more_state(self):
        proc_view, pods_view, pods_block = tui.dashboard_row_budget(
            height=33, fixed_top=16, proc_rows=10, pod_rows=8,
            pod_header_rows=2)
        self.assertEqual(proc_view, 10)
        self.assertEqual(pods_view, 3)
        self.assertEqual(pods_block, 6)

    def test_very_short_screen_hides_pods(self):
        proc_view, pods_view, pods_block = tui.dashboard_row_budget(
            height=23, fixed_top=16, proc_rows=10, pod_rows=8,
            pod_header_rows=2)
        self.assertGreaterEqual(proc_view, 1)
        self.assertEqual(pods_view, 0)
        self.assertEqual(pods_block, 0)


if __name__ == "__main__":
    unittest.main()
