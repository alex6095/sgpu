"""Tests for the TUI activity spinners (pure, curses-free helpers)."""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tools",
                                "gpu-monitor"))

import tui       # noqa: E402
import render    # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
