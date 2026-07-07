"""Unit tests for statsagg (aggregation math + rendering)."""

import gzip
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

# --- sys.path shim: make tools/gpu-monitor importable ---
_HERE = os.path.dirname(os.path.abspath(__file__))
_MOD_DIR = os.path.join(_HERE, "..", "tools", "gpu-monitor")
sys.path.insert(0, os.path.abspath(_MOD_DIR))

import statsagg  # noqa: E402


def ts_at(base_epoch, offset_s):
    dt = datetime.fromtimestamp(base_epoch + offset_s, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def write_day(d, date_str, records, gzipped=False):
    """Write a list of sample dicts as JSONL; return the path."""
    name = "samples-%s.jsonl" % date_str
    path = os.path.join(d, name)
    text = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in records)
    if gzipped:
        path += ".gz"
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            fh.write(text)
    else:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
    return path


def sample(ts, gpus=None, procs=None, pods=None):
    r = {"v": 1, "ts": ts, "node": "n1", "gpus": gpus or [], "procs": procs or []}
    if pods is not None:
        r["pods"] = pods
    return r


def gpu(i, util=None, mem_total=143771, name="NVIDIA H200"):
    return {"i": i, "util": util, "mem_total": mem_total, "name": name}


def proc(pid, gi, owner, mem=None, sm=None, pod=None):
    return {"pid": pid, "gpu": gi, "owner": owner, "mem": mem, "sm": sm,
            "pod": pod}


class TestDtGapRule(unittest.TestCase):
    def test_gap_credit_bounded(self):
        # 3 samples 15s apart, then a 2h gap, then 1 more.
        base = 1751860000  # arbitrary epoch, aligned to a second
        recs = [
            sample(ts_at(base, 0), gpus=[gpu(0, 50)],
                   procs=[proc(1, 0, "a", sm=90)]),
            sample(ts_at(base, 15), gpus=[gpu(0, 50)],
                   procs=[proc(1, 0, "a", sm=90)]),
            sample(ts_at(base, 30), gpus=[gpu(0, 50)],
                   procs=[proc(1, 0, "a", sm=90)]),
            sample(ts_at(base, 30 + 7200), gpus=[gpu(0, 50)],
                   procs=[proc(1, 0, "a", sm=90)]),
        ]
        with tempfile.TemporaryDirectory() as d:
            path = write_day(d, "20260707", recs)
            roll = statsagg.build_rollup(path)
        self.assertEqual(roll["samples"], 4)
        self.assertEqual(roll["interval_hint"], 15)
        # dt: 15 (first) + 15 + 15 + min(7200, 30) = 75
        self.assertAlmostEqual(roll["coverage_seconds"], 75.0, places=6)
        # owner "a" active on 1 gpu each sample -> gpu_seconds == coverage
        self.assertAlmostEqual(roll["owners"]["a"]["gpu_seconds"], 75.0,
                               places=6)


class TestSharedGpuUtil(unittest.TestCase):
    def test_two_owners_share_one_gpu(self):
        # One gpu, util 80, two owners each with a proc on it. Single sample.
        base = 1751860000
        recs = [
            sample(ts_at(base, 0), gpus=[gpu(0, 80)],
                   procs=[proc(1, 0, "a"), proc(2, 0, "b")]),
        ]
        with tempfile.TemporaryDirectory() as d:
            path = write_day(d, "20260707", recs)
            roll = statsagg.build_rollup(path)
        # First sample dt = interval_hint (fallback 15, no gaps).
        dt = roll["interval_hint"]
        a = roll["owners"]["a"]
        b = roll["owners"]["b"]
        # Both charged gpu_seconds for the shared gpu.
        self.assertAlmostEqual(a["gpu_seconds"], 1 * dt, places=6)
        self.assertAlmostEqual(b["gpu_seconds"], 1 * dt, places=6)
        # Device util split by 2 sharers: 80*dt/2 each.
        self.assertAlmostEqual(a["util_wsum"], 80 * dt / 2, places=6)
        self.assertAlmostEqual(a["util_weight"], dt, places=6)
        self.assertAlmostEqual(b["util_wsum"], 80 * dt / 2, places=6)


class TestIdleAllocation(unittest.TestCase):
    def test_idle_when_pod_uses_fewer_gpus(self):
        # Pod req=4 but procs only on 1 gpu -> idle 3*dt.
        base = 1751860000
        pods = [{"pod": "a-x", "owner": "a", "req": 4, "phase": "Running",
                 "start": ts_at(base, -3600)}]
        recs = [
            sample(ts_at(base, 0), gpus=[gpu(0, 50)],
                   procs=[proc(1, 0, "a", pod="a-x")], pods=pods),
        ]
        with tempfile.TemporaryDirectory() as d:
            path = write_day(d, "20260707", recs)
            roll = statsagg.build_rollup(path)
        dt = roll["interval_hint"]
        a = roll["owners"]["a"]
        self.assertAlmostEqual(a["alloc_gpu_seconds"], 4 * dt, places=6)
        self.assertAlmostEqual(a["idle_gpu_seconds"], 3 * dt, places=6)
        self.assertAlmostEqual(roll["pods_coverage_seconds"], dt, places=6)


class TestKstBinning(unittest.TestCase):
    def test_ts_2330_utc_maps_to_kst_hour_8(self):
        # 23:30 UTC + 9h = 08:30 next day -> KST hour 8.
        recs = [
            sample("2026-07-07T23:30:00Z", gpus=[gpu(0, 50)],
                   procs=[proc(1, 0, "a")]),
        ]
        with tempfile.TemporaryDirectory() as d:
            path = write_day(d, "20260707", recs)
            roll = statsagg.build_rollup(path)
        hist = roll["owners"]["a"]["hour_hist_kst"]
        dt = roll["interval_hint"]
        self.assertAlmostEqual(hist[8], 1 * dt, places=6)
        for h in range(24):
            if h != 8:
                self.assertEqual(hist[h], 0.0)


class TestMergeEqualsConcat(unittest.TestCase):
    def test_merge_two_days_equals_build_over_concat(self):
        # The two groups are contiguous (group 2 starts exactly one interval
        # after group 1's last sample) so that in the concatenated build the
        # first sample of group 2 is credited dt = min(15, 2*15) = 15, matching
        # the first-of-day credit it receives when group 2 is built alone. This
        # is the condition under which merge(day1,day2) == build(day1+day2).
        base = 1751860000  # some time
        day1 = [
            sample(ts_at(base, 0), gpus=[gpu(0, 60)],
                   procs=[proc(1, 0, "a", sm=70, mem=1000)]),
            sample(ts_at(base, 15), gpus=[gpu(0, 60)],
                   procs=[proc(1, 0, "a", sm=70, mem=1000)]),
        ]
        base2 = base + 30  # 15s after day1's last sample -> contiguous
        day2 = [
            sample(ts_at(base2, 0), gpus=[gpu(1, 40)],
                   procs=[proc(2, 1, "a", sm=30, mem=500),
                          proc(3, 1, "b", sm=None, mem=200)]),
            sample(ts_at(base2, 15), gpus=[gpu(1, 40)],
                   procs=[proc(2, 1, "a", sm=30, mem=500),
                          proc(3, 1, "b", sm=None, mem=200)]),
        ]
        with tempfile.TemporaryDirectory() as d:
            p1 = write_day(d, "20260707", day1)
            p2 = write_day(d, "20260708", day2)
            r1 = statsagg.build_rollup(p1)
            r2 = statsagg.build_rollup(p2)
            merged = statsagg.merge_rollups([r1, r2])

            # Build over the concatenation of both days (same interval hint 15).
            pc = write_day(d, "20260709", day1 + day2)
            concat = statsagg.build_rollup(pc)

        self.assertAlmostEqual(merged["coverage_seconds"],
                               concat["coverage_seconds"], places=5)
        for owner in ("a", "b"):
            m = merged["owners"][owner]
            c = concat["owners"][owner]
            for key in ("gpu_seconds", "sm_wsum", "sm_weight", "util_wsum",
                        "util_weight", "mem_wsum", "mem_weight"):
                self.assertAlmostEqual(m[key], c[key], places=5,
                                       msg="%s.%s" % (owner, key))
            self.assertEqual(m["mem_peak_mib"], c["mem_peak_mib"])
            for h in range(24):
                self.assertAlmostEqual(m["hour_hist_kst"][h],
                                       c["hour_hist_kst"][h], places=5)


class TestMalformedLineSkipped(unittest.TestCase):
    def test_truncated_trailing_line(self):
        base = 1751860000
        good = sample(ts_at(base, 0), gpus=[gpu(0, 50)],
                      procs=[proc(1, 0, "a")])
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "samples-20260707.jsonl")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(good, separators=(",", ":")) + "\n")
                fh.write('{"v":1,"ts":"2026-07-07T00:00:15Z","gpus":[{"i":0')
            roll = statsagg.build_rollup(path)
        # Only the one complete line counted.
        self.assertEqual(roll["samples"], 1)


class TestGzipTransparent(unittest.TestCase):
    def test_reads_gz(self):
        base = 1751860000
        recs = [sample(ts_at(base, 0), gpus=[gpu(0, 50)],
                       procs=[proc(1, 0, "a")])]
        with tempfile.TemporaryDirectory() as d:
            path = write_day(d, "20260707", recs, gzipped=True)
            roll = statsagg.build_rollup(path)
        self.assertEqual(roll["samples"], 1)


class TestCurrentIdle(unittest.TestCase):
    def test_detects_running_pod_with_no_procs(self):
        # Today's samples in the last 30 min: pod running, zero procs on it.
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        now_epoch = now.timestamp()
        pods = [{"pod": "z-idle", "owner": "z", "req": 4, "phase": "Running",
                 "start": "2026-07-07T09:00:00Z"}]
        recs = [
            sample(ts_at(now_epoch, -600), gpus=[gpu(0, 0)], procs=[],
                   pods=pods),
            sample(ts_at(now_epoch, -300), gpus=[gpu(0, 0)], procs=[],
                   pods=pods),
        ]
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260707", recs)
            result = statsagg.query(d, days=1, now_fn=lambda: now)
        idle = result["current_idle"]
        self.assertEqual(len(idle), 1)
        self.assertEqual(idle[0]["pod"], "z-idle")
        self.assertEqual(idle[0]["owner"], "z")
        self.assertEqual(idle[0]["req"], 4)
        self.assertGreaterEqual(idle[0]["idle_minutes"], 9)

    def test_pod_with_proc_not_idle(self):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        now_epoch = now.timestamp()
        pods = [{"pod": "z-busy", "owner": "z", "req": 4, "phase": "Running",
                 "start": "2026-07-07T09:00:00Z"}]
        recs = [
            sample(ts_at(now_epoch, -600), gpus=[gpu(0, 90)],
                   procs=[proc(1, 0, "z", pod="z-busy")], pods=pods),
        ]
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260707", recs)
            result = statsagg.query(d, days=1, now_fn=lambda: now)
        self.assertEqual(result["current_idle"], [])


class TestQueryAndRender(unittest.TestCase):
    def test_query_empty_dir(self):
        with tempfile.TemporaryDirectory() as d:
            now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
            result = statsagg.query(d, days=7, now_fn=lambda: now)
        self.assertEqual(result["merged"]["samples"], 0)
        self.assertEqual(result["dates_covered"], [])
        self.assertEqual(result["current_idle"], [])
        text = statsagg.render_stats_text(result)
        self.assertIn("no samples recorded yet", text)

    def test_render_smoke_with_owners(self):
        base = 1751860000
        pods = [{"pod": "yoonki-x", "owner": "yoonki", "req": 4,
                 "phase": "Running", "start": ts_at(base, -3600)}]
        recs = []
        for i in range(400):  # enough gpu-hours to trip LOW-UTIL
            recs.append(sample(
                ts_at(base, i * 15),
                gpus=[gpu(0, 10)],
                procs=[proc(1, 0, "yoonki", sm=10, mem=1000, pod="yoonki-x")],
                pods=pods))
        now = datetime.fromtimestamp(base + 400 * 15, tz=timezone.utc)
        with tempfile.TemporaryDirectory() as d:
            # Put it on "today" so query reads it as raw.
            date_str = now.strftime("%Y%m%d")
            write_day(d, date_str, recs)
            result = statsagg.query(d, days=1, now_fn=lambda: now)
        text = statsagg.render_stats_text(result, color=False)
        self.assertIn("yoonki", text)
        self.assertIn("SGPU usage report", text)
        self.assertIn("KST", text)
        # Color variant must not raise and must differ (ANSI codes present).
        ctext = statsagg.render_stats_text(result, color=True)
        self.assertIn("\x1b[", ctext)

    def test_list_files(self):
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260706", [sample("2026-07-06T00:00:00Z")])
            with open(os.path.join(d, "rollup-20260706.json"), "w",
                      encoding="utf-8") as fh:
                fh.write("{}")
            with open(os.path.join(d, "ignore.txt"), "w",
                      encoding="utf-8") as fh:
                fh.write("x")
            files = statsagg.list_files(d)
            names = [f["name"] for f in files]
            self.assertIn("samples-20260706.jsonl", names)
            self.assertIn("rollup-20260706.json", names)
            self.assertNotIn("ignore.txt", names)
            for f in files:
                self.assertIn("size", f)
                self.assertIn("mtime_iso", f)

    def test_iter_raw_lines_missing_raises(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError):
                list(statsagg.iter_raw_lines(d, "20990101"))

    def test_iter_raw_lines_reads_gz(self):
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260706", [sample("2026-07-06T00:00:00Z")],
                      gzipped=True)
            lines = list(statsagg.iter_raw_lines(d, "20260706"))
            self.assertEqual(len(lines), 1)
            self.assertIn("2026-07-06T00:00:00Z", lines[0])


class TestDailySeries(unittest.TestCase):
    """query() returns a per-day series (oldest->newest) reusing rollups."""

    def test_daily_series_two_days(self):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        # yesterday: owner "a" only; today: owners "a" and "b".
        y = "20260706"
        t = "20260707"
        ybase = int(datetime(2026, 7, 6, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        tbase = int(datetime(2026, 7, 7, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        yrecs = [sample(ts_at(ybase, i * 15), gpus=[gpu(0, 50)],
                        procs=[proc(1, 0, "a")]) for i in range(4)]
        trecs = []
        for i in range(4):
            trecs.append(sample(ts_at(tbase, i * 15),
                                gpus=[gpu(0, 50), gpu(1, 50)],
                                procs=[proc(1, 0, "a"), proc(2, 1, "b")]))
        with tempfile.TemporaryDirectory() as d:
            write_day(d, y, yrecs)
            write_day(d, t, trecs)
            result = statsagg.query(d, days=2, now_fn=lambda: now)

        daily = result["daily"]
        self.assertEqual([e["date"] for e in daily], [y, t])  # oldest->newest
        # day1 only has "a"; day2 has "a" and "b".
        self.assertIn("a", daily[0]["owners"])
        self.assertNotIn("b", daily[0]["owners"])
        self.assertIn("a", daily[1]["owners"])
        self.assertIn("b", daily[1]["owners"])
        # gpu_seconds sums match the merged per-owner totals.
        merged = result["merged"]["owners"]
        a_daily = sum(e["owners"].get("a", 0.0) for e in daily)
        self.assertAlmostEqual(a_daily, merged["a"]["gpu_seconds"], places=6)
        b_daily = sum(e["owners"].get("b", 0.0) for e in daily)
        self.assertAlmostEqual(b_daily, merged["b"]["gpu_seconds"], places=6)
        # coverage_seconds present and positive per day.
        for e in daily:
            self.assertGreater(e["coverage_seconds"], 0.0)

    def test_daily_owner_filter(self):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        tbase = int(datetime(2026, 7, 7, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        recs = [sample(ts_at(tbase, i * 15), gpus=[gpu(0, 50), gpu(1, 50)],
                       procs=[proc(1, 0, "a"), proc(2, 1, "b")])
                for i in range(4)]
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260707", recs)
            result = statsagg.query(d, days=1, owner="a", now_fn=lambda: now)
        # daily owners restricted to "a" only.
        for e in result["daily"]:
            self.assertEqual(list(e["owners"].keys()), ["a"])

    def test_query_backward_compatible_keys(self):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as d:
            result = statsagg.query(d, days=7, now_fn=lambda: now)
        for k in ("window_days", "dates_covered", "merged", "current_idle",
                  "generated_utc", "daily"):
            self.assertIn(k, result)


class TestGrassBucketing(unittest.TestCase):
    def test_bucket_levels(self):
        # row max maps to top level (5); zero maps to empty (0).
        self.assertEqual(statsagg._bucket_level(0.0, 100.0, 5), 0)
        self.assertEqual(statsagg._bucket_level(100.0, 100.0, 5), 5)
        # tiny positive -> level 1 (never skips to 0).
        self.assertEqual(statsagg._bucket_level(0.01, 100.0, 5), 1)
        # mid maps into 1..5.
        self.assertTrue(1 <= statsagg._bucket_level(50.0, 100.0, 5) <= 5)
        # mx == 0 -> level 0 regardless.
        self.assertEqual(statsagg._bucket_level(5.0, 0.0, 5), 0)

    def test_grass_present_only_when_two_dates(self):
        # one date -> no calendar.
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        tbase = int(datetime(2026, 7, 7, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        recs = [sample(ts_at(tbase, i * 15), gpus=[gpu(0, 50)],
                       procs=[proc(1, 0, "a")]) for i in range(4)]
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260707", recs)
            result = statsagg.query(d, days=1, now_fn=lambda: now)
        text = statsagg.render_stats_text(result)
        self.assertNotIn("Daily activity", text)

    def test_grass_renders_with_two_dates(self):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        ybase = int(datetime(2026, 7, 6, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        tbase = int(datetime(2026, 7, 7, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        yrecs = [sample(ts_at(ybase, i * 15), gpus=[gpu(0, 50)],
                        procs=[proc(1, 0, "a")]) for i in range(4)]
        trecs = [sample(ts_at(tbase, i * 15), gpus=[gpu(0, 50)],
                        procs=[proc(1, 0, "a")]) for i in range(4)]
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260706", yrecs)
            write_day(d, "20260707", trecs)
            result = statsagg.query(d, days=2, now_fn=lambda: now)
        text = statsagg.render_stats_text(result, unicode_ok=True)
        self.assertIn("Daily activity", text)
        self.assertIn("less", text)  # legend

    def test_grass_fills_gap_dates_as_empty(self):
        # Data on 07-01 and 07-07 only; calendar spans all 7 dates, gaps empty.
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as d:
            for date_str in ("20260701", "20260707"):
                base = int(datetime(2026, int(date_str[4:6]),
                                    int(date_str[6:8]), 1, 0, 0,
                                    tzinfo=timezone.utc).timestamp())
                recs = [sample(ts_at(base, i * 15), gpus=[gpu(0, 50)],
                               procs=[proc(1, 0, "a")]) for i in range(4)]
                write_day(d, date_str, recs)
            result = statsagg.query(d, days=7, now_fn=lambda: now)
        # only two data dates, but calendar spans 7 contiguous dates.
        self.assertEqual(len(result["daily"]), 2)
        text = statsagg.render_stats_text(result, width=110, unicode_ok=True)
        lines = text.splitlines()
        start = lines.index("Daily activity")
        row = None
        for l in lines[start:]:
            if l.startswith("a "):
                row = l
                break
        self.assertIsNotNone(row)
        # 7 cells x 2 chars = 14 cols of cells after the label(width=8)+space.
        cells = row[9:]  # ow=8 label + 1 space
        self.assertEqual(len(cells), 14)  # 7 dates -> 14 columns
        # first and last cells filled; middle empty (spaces).
        self.assertNotEqual(cells[0:2], "  ")   # 07-01 has data
        self.assertNotEqual(cells[12:14], "  ")  # 07-07 has data
        self.assertEqual(cells[2:12], " " * 10)  # 5 gap days empty

    def test_grass_width_bound_and_truncation(self):
        # Many dates, narrow width -> most-recent kept, '…' prefix, within width.
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as d:
            for back in range(14):
                dd = now.date().fromordinal(now.date().toordinal() - back)
                base = int(datetime(dd.year, dd.month, dd.day, 1, 0, 0,
                                    tzinfo=timezone.utc).timestamp())
                recs = [sample(ts_at(base, i * 15), gpus=[gpu(0, 50)],
                               procs=[proc(1, 0, "a")]) for i in range(4)]
                write_day(d, dd.strftime("%Y%m%d"), recs)
            result = statsagg.query(d, days=14, now_fn=lambda: now)
        width = 30
        text = statsagg.render_stats_text(result, width=width, unicode_ok=True)
        # isolate the "Daily activity" section (up to the next blank line).
        all_lines = text.splitlines()
        start = all_lines.index("Daily activity")
        section = []
        for l in all_lines[start:]:
            if l == "" and section:
                break
            section.append(l)
        # the grass owner/total rows are those containing a grass glyph.
        grass_lines = [l for l in section
                       if any(g in l for g in "…░▒▓█") or l.strip() == "TOTAL"]
        self.assertTrue(grass_lines)
        for l in grass_lines:
            self.assertLessEqual(len(l), width)
        self.assertIn("…", text)  # truncation marker present


class TestPluralFix(unittest.TestCase):
    def test_singular_one_day(self):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        tbase = int(datetime(2026, 7, 7, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        recs = [sample(ts_at(tbase, i * 15), gpus=[gpu(0, 50)],
                       procs=[proc(1, 0, "a")]) for i in range(4)]
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260707", recs)
            result = statsagg.query(d, days=1, now_fn=lambda: now)
        text = statsagg.render_stats_text(result)
        # subtitle uses singular "1 day" (bug fix); title keeps "last N days".
        self.assertIn("data: 1 day,", text)
        self.assertNotIn("data: 1 days", text)

    def test_plural_two_days(self):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        ybase = int(datetime(2026, 7, 6, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        tbase = int(datetime(2026, 7, 7, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260706", [sample(ts_at(ybase, i * 15),
                      gpus=[gpu(0, 50)], procs=[proc(1, 0, "a")])
                      for i in range(4)])
            write_day(d, "20260707", [sample(ts_at(tbase, i * 15),
                      gpus=[gpu(0, 50)], procs=[proc(1, 0, "a")])
                      for i in range(4)])
            result = statsagg.query(d, days=2, now_fn=lambda: now)
        text = statsagg.render_stats_text(result)
        self.assertIn("data: 2 days,", text)


def _acc(gpu_seconds=0.0, sm_wsum=0.0, sm_weight=0.0, util_wsum=0.0,
         util_weight=0.0, mem_peak_mib=0, alloc_gpu_seconds=0.0,
         idle_gpu_seconds=0.0, night_frac=0.0):
    a = statsagg._empty_owner()
    a["gpu_seconds"] = gpu_seconds
    a["sm_wsum"] = sm_wsum
    a["sm_weight"] = sm_weight
    a["util_wsum"] = util_wsum
    a["util_weight"] = util_weight
    a["mem_peak_mib"] = mem_peak_mib
    a["alloc_gpu_seconds"] = alloc_gpu_seconds
    a["idle_gpu_seconds"] = idle_gpu_seconds
    # spread activity: night_frac of gpu_seconds into KST hours 0-5.
    hh = [0.0] * 24
    hh[2] = gpu_seconds * night_frac
    hh[14] = gpu_seconds * (1.0 - night_frac)
    a["hour_hist_kst"] = hh
    return a


class TestAwards(unittest.TestCase):
    def test_below_threshold_gets_no_award(self):
        # single owner with < 1 GPU-h and low util: no best/power award.
        owners = {"tiny": _acc(gpu_seconds=1800.0,  # 0.5 GPU-h
                               sm_wsum=10 * 1800.0, sm_weight=1800.0)}
        awards = statsagg._compute_awards(owners, pods_cov=0.0,
                                          coverage_seconds=1800.0)
        keys = [k for (k, _o, _t) in awards]
        self.assertNotIn("best", keys)   # < 1 GPU-h
        self.assertNotIn("power", keys)  # < 1 GPU-h
        self.assertNotIn("sharp", keys)  # < 2 GPU-h

    def test_best_requires_40pct_util(self):
        # 10 GPU-h but util 30% -> not "best"; still "power".
        owners = {"a": _acc(gpu_seconds=36000.0,
                            sm_wsum=30 * 36000.0, sm_weight=36000.0)}
        awards = dict((k, (o, t)) for (k, o, t) in
                      statsagg._compute_awards(owners, 0.0, 36000.0))
        self.assertNotIn("best", awards)
        self.assertIn("power", awards)

    def test_best_awarded_when_util_high(self):
        owners = {"a": _acc(gpu_seconds=36000.0,   # 10 GPU-h
                            sm_wsum=70 * 36000.0, sm_weight=36000.0,
                            mem_peak_mib=60000)}
        awards = dict((k, o) for (k, o, t) in
                      statsagg._compute_awards(owners, 0.0, 36000.0))
        self.assertEqual(awards.get("best"), "a")

    def test_memory_threshold_32gib(self):
        # memowner has the peak mem; other owner wins the compute awards so the
        # 3-per-owner cap does not hide the mem award.
        other = _acc(gpu_seconds=36000.0, sm_wsum=90 * 36000.0,
                     sm_weight=36000.0)
        # 30 GiB peak -> no mem award for anyone.
        low = {"other": other,
               "memowner": _acc(gpu_seconds=3600.0, sm_wsum=50 * 3600.0,
                                sm_weight=3600.0, mem_peak_mib=30 * 1024)}
        keys_low = [k for (k, _o, _t) in
                    statsagg._compute_awards(low, 0.0, 39600.0)]
        self.assertNotIn("mem", keys_low)
        # 40 GiB peak -> mem awarded to memowner.
        high = {"other": other,
                "memowner": _acc(gpu_seconds=3600.0, sm_wsum=50 * 3600.0,
                                 sm_weight=3600.0, mem_peak_mib=40 * 1024)}
        awards = dict((k, o) for (k, o, _t) in
                      statsagg._compute_awards(high, 0.0, 39600.0))
        self.assertEqual(awards.get("mem"), "memowner")

    def test_night_owl(self):
        # "top" sweeps the compute awards; "owl" wins night by working 0-5 KST.
        owners = {
            "top": _acc(gpu_seconds=72000.0, sm_wsum=90 * 72000.0,
                        sm_weight=72000.0, mem_peak_mib=80 * 1024,
                        night_frac=0.1),
            "owl": _acc(gpu_seconds=18000.0, sm_wsum=50 * 18000.0,
                        sm_weight=18000.0, night_frac=0.9),
        }
        awards = dict((k, o) for (k, o, t) in
                      statsagg._compute_awards(owners, 0.0, 90000.0))
        self.assertEqual(awards.get("night"), "owl")

    def test_headroom_kind_phrasing(self):
        # >= 4 GPU-h, util < 40% -> headroom, kind phrasing.
        owners = {"a": _acc(gpu_seconds=4 * 3600.0,
                            util_wsum=22 * 4 * 3600.0,
                            util_weight=4 * 3600.0)}
        out = statsagg._compute_awards(owners, 0.0, 4 * 3600.0)
        texts = {k: t for (k, o, t) in out}
        self.assertIn("headroom", texts)
        self.assertIn("free speedup waiting", texts["headroom"])

    def test_seat_warmer_requires_pod_coverage(self):
        # "top" sweeps compute awards; "idler" is the seat warmer.
        owners = {
            "top": _acc(gpu_seconds=72000.0, sm_wsum=90 * 72000.0,
                        sm_weight=72000.0, mem_peak_mib=80 * 1024),
            "idler": _acc(gpu_seconds=18000.0, sm_wsum=50 * 18000.0,
                          sm_weight=18000.0, idle_gpu_seconds=3 * 3600.0),
        }
        # pods_cov below 0.5*coverage -> no seat award.
        keys_no = [k for (k, _o, _t) in
                   statsagg._compute_awards(owners, pods_cov=100.0,
                                            coverage_seconds=90000.0)]
        self.assertNotIn("seat", keys_no)
        # strong pod coverage -> seat award to idler.
        awards_yes = dict((k, o) for (k, o, _t) in
                          statsagg._compute_awards(owners, pods_cov=60000.0,
                                                   coverage_seconds=90000.0))
        self.assertEqual(awards_yes.get("seat"), "idler")

    def test_max_three_awards_per_owner(self):
        # One dominant owner that would win 5 awards -> capped at 3.
        owners = {
            "star": _acc(gpu_seconds=36000.0, sm_wsum=90 * 36000.0,
                         sm_weight=36000.0, mem_peak_mib=80 * 1024,
                         night_frac=0.9),
            "b": _acc(gpu_seconds=3600.0, sm_wsum=50 * 3600.0,
                      sm_weight=3600.0, mem_peak_mib=1),
        }
        awards = statsagg._compute_awards(owners, 0.0, 36000.0)
        star_count = sum(1 for (_k, o, _t) in awards if o == "star")
        self.assertLessEqual(star_count, 3)
        # The dropped ones are lowest priority (night dropped, best kept).
        keys_for_star = [k for (k, o, _t) in awards if o == "star"]
        self.assertIn("best", keys_for_star)

    def test_empty_owners_no_crash(self):
        self.assertEqual(statsagg._compute_awards({}, 0.0, 0.0), [])
        # only the "?" owner -> excluded, no awards, no crash.
        owners = {"?": _acc(gpu_seconds=36000.0, sm_wsum=70 * 36000.0,
                            sm_weight=36000.0)}
        self.assertEqual(statsagg._compute_awards(owners, 0.0, 36000.0), [])


class TestUnicodeAsciiModes(unittest.TestCase):
    def _two_day_result(self, d):
        now = datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)
        ybase = int(datetime(2026, 7, 6, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        tbase = int(datetime(2026, 7, 7, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        for date_str, base in (("20260706", ybase), ("20260707", tbase)):
            recs = [sample(ts_at(base, i * 15),
                           gpus=[gpu(0, 80)],
                           procs=[proc(1, 0, "yoonki", sm=75, mem=60000)])
                    for i in range(300)]
            write_day(d, date_str, recs)
        return statsagg.query(d, days=2, now_fn=lambda: now)

    def test_unicode_false_no_emoji_no_ansi(self):
        with tempfile.TemporaryDirectory() as d:
            result = self._two_day_result(d)
        text = statsagg.render_stats_text(result, color=False,
                                          unicode_ok=False)
        self.assertNotIn("\x1b", text)  # no ANSI
        # no emoji code points anywhere.
        for ch in text:
            self.assertLess(ord(ch), 0x2500,
                            msg="unexpected non-ascii glyph %r" % ch)
        # ascii award tag used instead of emoji.
        self.assertIn("[best]", text)
        self.assertIn("[power]", text)

    def test_unicode_true_has_emoji(self):
        with tempfile.TemporaryDirectory() as d:
            result = self._two_day_result(d)
        text = statsagg.render_stats_text(result, color=False,
                                          unicode_ok=True)
        self.assertIn("🏆", text)

    def test_color_true_has_ansi_backgrounds(self):
        with tempfile.TemporaryDirectory() as d:
            result = self._two_day_result(d)
        text = statsagg.render_stats_text(result, color=True, unicode_ok=True)
        self.assertIn("\x1b[48;5;", text)  # grass 256-color backgrounds
        # The KST heatmap uses the same background-cell language as the
        # grass: empty hours get the dark base cell so rows read as one
        # contiguous strip (never sparse foreground sparks).
        heat = text.split("KST hour activity", 1)[1]
        self.assertIn("\x1b[48;5;238m", heat)   # empty-hour base cells
        self.assertNotIn("\x1b[38;5;", heat)    # no fg sparks anymore

    def test_leaderboard_full_names(self):
        # owner names longer than 3 chars must appear in full in the report.
        with tempfile.TemporaryDirectory() as d:
            result = self._two_day_result(d)
        text = statsagg.render_stats_text(result, color=False)
        self.assertIn("yoonki", text)  # not truncated to "yoo"


class TestLabMerge(unittest.TestCase):
    """Lab-wide (multi-node) merge of per-node query() results."""

    def _now(self):
        return datetime(2026, 7, 7, 12, 0, 0, tzinfo=timezone.utc)

    def _tbase(self):
        return int(datetime(2026, 7, 7, 1, 0, 0,
                            tzinfo=timezone.utc).timestamp())

    def _node02(self, d, n_shared=200):
        # node-02: "sujin" (only here) + "shared" (n_shared samples here).
        base = self._tbase()
        recs = []
        for i in range(300):
            procs = [proc(1, 0, "sujin", sm=75, mem=60000)]
            if i < n_shared:
                procs.append(proc(2, 1, "shared", sm=60, mem=50000))
            recs.append(sample(ts_at(base, i * 15),
                               gpus=[gpu(0, 80), gpu(1, 70)], procs=procs))
        write_day(d, "20260707", recs)

    def _node01(self, d, n_shared=200):
        # node-01: "minho" (only here) + "shared" (n_shared samples here).
        base = self._tbase()
        recs = []
        for i in range(150):
            procs = [proc(1, 0, "minho", sm=30, mem=20000)]
            if i < n_shared:
                procs.append(proc(2, 0, "shared", sm=55, mem=15000))
            recs.append(sample(ts_at(base, i * 15),
                               gpus=[gpu(0, 40)], procs=procs))
        write_day(d, "20260707", recs)

    def _merge(self, n_shared_02=200, n_shared_01=200):
        now = self._now()
        with tempfile.TemporaryDirectory() as d1, \
                tempfile.TemporaryDirectory() as d2:
            self._node02(d1, n_shared=n_shared_02)
            self._node01(d2, n_shared=n_shared_01)
            local = statsagg.query(d1, days=1, now_fn=lambda: now)
            peer = statsagg.query(d2, days=1, now_fn=lambda: now)
        lab = statsagg.merge_query_results([("node-02", local),
                                            ("node-01", peer)])
        return local, peer, lab

    def test_shared_owner_sums_disjoint_preserved(self):
        local, peer, lab = self._merge()
        owners = lab["merged"]["owners"]
        # disjoint owners preserved.
        self.assertIn("sujin", owners)
        self.assertIn("minho", owners)
        self.assertIn("shared", owners)
        # shared owner's gpu_seconds = sum of both nodes' contributions.
        s_local = local["merged"]["owners"]["shared"]["gpu_seconds"]
        s_peer = peer["merged"]["owners"]["shared"]["gpu_seconds"]
        self.assertAlmostEqual(owners["shared"]["gpu_seconds"],
                               s_local + s_peer, places=6)
        # sujin only on node-02, minho only on node-01.
        self.assertAlmostEqual(
            owners["sujin"]["gpu_seconds"],
            local["merged"]["owners"]["sujin"]["gpu_seconds"], places=6)
        self.assertAlmostEqual(
            owners["minho"]["gpu_seconds"],
            peer["merged"]["owners"]["minho"]["gpu_seconds"], places=6)

    def test_owner_nodes_and_node_column(self):
        # Give the shared owner meaningful time on both nodes (not lopsided).
        local, peer, lab = self._merge()
        on = lab["owner_nodes"]
        # owner_nodes carries per-node gpu_seconds from each merged.owners.
        self.assertAlmostEqual(
            on["shared"]["node-02"],
            local["merged"]["owners"]["shared"]["gpu_seconds"], places=6)
        self.assertAlmostEqual(
            on["shared"]["node-01"],
            peer["merged"]["owners"]["shared"]["gpu_seconds"], places=6)
        self.assertEqual(set(on["sujin"].keys()), {"node-02"})
        self.assertEqual(set(on["minho"].keys()), {"node-01"})

        text = statsagg.render_stats_text(lab, color=False)
        self.assertIn("NODE", text)
        # Isolate the leaderboard section (from "Leaderboard" to blank line) so
        # we don't pick up KST heatmap rows that also start with an owner name.
        all_lines = text.splitlines()
        start = all_lines.index("Leaderboard")
        rows = {}
        for l in all_lines[start + 2:]:  # skip "Leaderboard" + header row
            if l == "":
                break
            parts = l.split()
            # Row layout: "<rank>. <OWNER> <NODE> <GPU-H> ..."
            if len(parts) >= 3 and parts[1] in ("sujin", "minho", "shared"):
                rows[parts[1]] = parts[2]  # NODE follows OWNER
        self.assertEqual(rows["sujin"], "node-02")   # single-node owner
        self.assertEqual(rows["minho"], "node-01")   # single-node owner
        self.assertEqual(rows["shared"], "both")     # split across both nodes

    def test_node_column_95pct_threshold(self):
        # Shared owner almost entirely on node-02 (>=95%) -> shows that label.
        # node-02 has 300 shared samples on 1 gpu; node-01 has ~5 -> ~98%.
        local, peer, lab = self._merge(n_shared_02=300, n_shared_01=5)
        label = statsagg._owner_node_label("shared", lab["owner_nodes"])
        self.assertEqual(label, "node-02")

    def test_daily_union_sum_and_max_coverage(self):
        local, peer, lab = self._merge()
        daily = lab["daily"]
        # single shared date.
        self.assertEqual([e["date"] for e in daily], ["20260707"])
        day = daily[0]
        # per-date owner sums across nodes.
        l0 = local["daily"][0]["owners"]
        p0 = peer["daily"][0]["owners"]
        expected_shared = l0.get("shared", 0.0) + p0.get("shared", 0.0)
        self.assertAlmostEqual(day["owners"]["shared"], expected_shared,
                               places=6)
        self.assertAlmostEqual(day["owners"]["sujin"], l0["sujin"], places=6)
        self.assertAlmostEqual(day["owners"]["minho"], p0["minho"], places=6)
        # coverage = MAX across nodes for the date (parallel monitors).
        expected_cov = max(local["daily"][0]["coverage_seconds"],
                           peer["daily"][0]["coverage_seconds"])
        self.assertAlmostEqual(day["coverage_seconds"], expected_cov, places=6)

    def test_daily_date_union_across_nodes(self):
        # node-02 has today; node-01 has yesterday+today -> union is both dates.
        now = self._now()
        ybase = int(datetime(2026, 7, 6, 1, 0, 0,
                             tzinfo=timezone.utc).timestamp())
        tbase = self._tbase()
        with tempfile.TemporaryDirectory() as d1, \
                tempfile.TemporaryDirectory() as d2:
            # node-02: only today.
            write_day(d1, "20260707",
                      [sample(ts_at(tbase, i * 15), gpus=[gpu(0, 50)],
                              procs=[proc(1, 0, "sujin")]) for i in range(4)])
            # node-01: yesterday and today.
            write_day(d2, "20260706",
                      [sample(ts_at(ybase, i * 15), gpus=[gpu(0, 50)],
                              procs=[proc(1, 0, "minho")]) for i in range(4)])
            write_day(d2, "20260707",
                      [sample(ts_at(tbase, i * 15), gpus=[gpu(0, 50)],
                              procs=[proc(1, 0, "minho")]) for i in range(4)])
            local = statsagg.query(d1, days=2, now_fn=lambda: now)
            peer = statsagg.query(d2, days=2, now_fn=lambda: now)
        lab = statsagg.merge_query_results([("node-02", local),
                                            ("node-01", peer)])
        self.assertEqual([e["date"] for e in lab["daily"]],
                         ["20260706", "20260707"])
        self.assertEqual(lab["dates_covered"], ["20260706", "20260707"])

    def test_awards_recomputed_over_merged_owners(self):
        # node-02 alone: sujin is the power user (most GPU-h locally).
        # node-01 alone: minho is the power user locally.
        # After merge, "shared" gets time from BOTH nodes -> flips power user.
        now = self._now()
        tbase = self._tbase()
        with tempfile.TemporaryDirectory() as d1, \
                tempfile.TemporaryDirectory() as d2:
            # node-02: sujin heavy, shared moderate.
            recs02 = [sample(ts_at(tbase, i * 15),
                             gpus=[gpu(0, 80), gpu(1, 70)],
                             procs=[proc(1, 0, "sujin", sm=75),
                                    proc(2, 1, "shared", sm=60)])
                      for i in range(300)]
            write_day(d1, "20260707", recs02)
            # node-01: minho heavy, shared moderate.
            recs01 = [sample(ts_at(tbase, i * 15),
                             gpus=[gpu(0, 40), gpu(1, 40)],
                             procs=[proc(1, 0, "minho", sm=30),
                                    proc(2, 1, "shared", sm=55)])
                      for i in range(300)]
            write_day(d2, "20260707", recs01)
            local = statsagg.query(d1, days=1, now_fn=lambda: now)
            peer = statsagg.query(d2, days=1, now_fn=lambda: now)

        # sanity: locally the power user is the single-node heavy owner.
        local_power = {a["key"]: a["owner"] for a in local["awards"]}
        self.assertEqual(local_power.get("power"), "sujin")

        lab = statsagg.merge_query_results([("node-02", local),
                                            ("node-01", peer)])
        lab_power = {a["key"]: a["owner"] for a in lab["awards"]}
        # "shared" now has GPU-h from both nodes -> most total -> power user.
        self.assertEqual(lab_power.get("power"), "shared")

    def test_current_idle_entries_carry_node(self):
        now = self._now()
        tbase = self._tbase()
        pods02 = [{"pod": "sujin-idle", "owner": "sujin", "req": 4,
                   "phase": "Running", "start": ts_at(tbase, -3600)}]
        pods01 = [{"pod": "minho-idle", "owner": "minho", "req": 2,
                   "phase": "Running", "start": ts_at(tbase, -3600)}]
        with tempfile.TemporaryDirectory() as d1, \
                tempfile.TemporaryDirectory() as d2:
            write_day(d1, "20260707", [
                sample(ts_at(now.timestamp(), -600), gpus=[gpu(0, 0)],
                       procs=[], pods=pods02),
                sample(ts_at(now.timestamp(), -300), gpus=[gpu(0, 0)],
                       procs=[], pods=pods02)])
            write_day(d2, "20260707", [
                sample(ts_at(now.timestamp(), -600), gpus=[gpu(0, 0)],
                       procs=[], pods=pods01),
                sample(ts_at(now.timestamp(), -300), gpus=[gpu(0, 0)],
                       procs=[], pods=pods01)])
            local = statsagg.query(d1, days=1, now_fn=lambda: now)
            peer = statsagg.query(d2, days=1, now_fn=lambda: now)
        lab = statsagg.merge_query_results([("node-02", local),
                                            ("node-01", peer)])
        idle = lab["current_idle"]
        by_pod = {e["pod"]: e for e in idle}
        self.assertEqual(by_pod["sujin-idle"]["node"], "node-02")
        self.assertEqual(by_pod["minho-idle"]["node"], "node-01")
        # IDLE-NOW warning line includes the node.
        text = statsagg.render_stats_text(lab, color=False)
        self.assertIn("IDLE-NOW pod sujin-idle (4 GPU, node-02)", text)

    def test_lab_title_and_notes(self):
        local, peer, lab = self._merge()
        lab["notes"] = ["node-01 unreachable: timeout"]
        text = statsagg.render_stats_text(lab, color=False)
        self.assertIn("all nodes", text)
        self.assertIn("(node-02+node-01)", text)
        # notes render at the very bottom.
        self.assertIn("node-01 unreachable: timeout", text)

    def test_non_lab_render_unchanged_no_node_column(self):
        # A plain (non-lab) query() result must render with no NODE column.
        now = self._now()
        tbase = self._tbase()
        with tempfile.TemporaryDirectory() as d:
            write_day(d, "20260707",
                      [sample(ts_at(tbase, i * 15), gpus=[gpu(0, 50)],
                              procs=[proc(1, 0, "a")]) for i in range(4)])
            result = statsagg.query(d, days=1, now_fn=lambda: now)
        text = statsagg.render_stats_text(result, color=False)
        self.assertNotIn("scope", result)
        self.assertNotIn("NODE", text)
        self.assertNotIn("all nodes", text)

    def test_single_entry_merge(self):
        # Only the local node -> still scope "lab" with one label.
        now = self._now()
        with tempfile.TemporaryDirectory() as d1:
            self._node02(d1)
            local = statsagg.query(d1, days=1, now_fn=lambda: now)
        lab = statsagg.merge_query_results([("node-02", local)])
        self.assertEqual(lab["scope"], "lab")
        self.assertEqual(lab["node_labels"], ["node-02"])
        # merged owners match the single node's owners.
        self.assertEqual(set(lab["merged"]["owners"].keys()),
                         set(local["merged"]["owners"].keys()))
        text = statsagg.render_stats_text(lab, color=False)
        self.assertIn("all nodes (node-02)", text)

    def test_malformed_entry_skipped_label_dropped(self):
        # A falsy/malformed result is skipped; its label is not in node_labels.
        now = self._now()
        with tempfile.TemporaryDirectory() as d1:
            self._node02(d1)
            local = statsagg.query(d1, days=1, now_fn=lambda: now)
        lab = statsagg.merge_query_results([
            ("node-02", local),
            ("node-01", None),      # unreachable peer -> falsy
            ("node-03", "garbage"),  # malformed -> not a dict
        ])
        self.assertEqual(lab["node_labels"], ["node-02"])
        # merge over the one valid result still works.
        self.assertIn("sujin", lab["merged"]["owners"])


if __name__ == "__main__":
    unittest.main()
