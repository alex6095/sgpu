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


if __name__ == "__main__":
    unittest.main()
