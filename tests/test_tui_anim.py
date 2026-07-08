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


class TestNonblockingInputPacing(unittest.TestCase):
    def test_frame_sleep_seconds_until_next_frame(self):
        self.assertAlmostEqual(
            tui.frame_sleep_seconds(10.0, 10.0), tui._FRAME_INTERVAL)
        self.assertGreater(tui.frame_sleep_seconds(
            10.01, 10.0), 0.0)
        self.assertEqual(tui.frame_sleep_seconds(
            10.0 + tui._FRAME_INTERVAL, 10.0), 0.0)


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
