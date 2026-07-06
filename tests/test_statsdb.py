"""Unit tests for statsdb (sample recording + rotation)."""

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

import statsdb  # noqa: E402
import statsagg  # noqa: E402


def make_snapshot(ts="2026-07-07T04:12:30Z", with_pods=True, pods_ok=True):
    snap = {
        "schema": 2,
        "source": "nvml",
        "time_utc": ts,
        "node": "h200-04-w-4b11",
        "driver": "580.126.16",
        "sgpu_version": "0.5.0",
        "gpus": [{
            "index": 0, "uuid": "GPU-abc", "name": "NVIDIA H200",
            "util": 97, "mem_used_mib": 121004, "mem_total_mib": 143771,
            "temp_c": 61, "power_w": 540.2, "power_limit_w": 700.0,
            "owners": ["yoonki"],
        }],
        "procs": [{
            "pid": 1234, "gpu_index": 0, "gpu_uuid": "GPU-abc",
            "mem_mib": 40213, "owner": "yoonki", "pod": "yoonki-ume-xvrdr",
            "pod_uid": None, "cmd": "x" * 200,
            "started_utc": "2026-07-06T09:00:00Z", "sm_util": 85,
            "attribution": "environ",
        }],
        "pods": {
            "ok": pods_ok, "source": "kubeconfig", "error": None,
            "rows": [{
                "owner": "yoonki", "pod": "yoonki-ume-xvrdr", "node": "n",
                "phase": "Running", "gpu": 4, "age": "19h7m",
                "uid": "u", "start_iso": "2026-07-06T09:00:00Z", "active": 4,
            }],
        },
    }
    if not with_pods:
        del snap["pods"]
    return snap


class TestSnapshotToSample(unittest.TestCase):
    def test_field_mapping(self):
        s = statsdb.snapshot_to_sample(make_snapshot())
        self.assertEqual(s["v"], 1)
        self.assertEqual(s["ts"], "2026-07-07T04:12:30Z")
        self.assertEqual(s["node"], "h200-04-w-4b11")
        self.assertEqual(s["driver"], "580.126.16")
        g = s["gpus"][0]
        self.assertEqual(g["i"], 0)
        self.assertEqual(g["uuid"], "GPU-abc")
        self.assertEqual(g["util"], 97)
        self.assertEqual(g["mem"], 121004)
        self.assertEqual(g["mem_total"], 143771)
        self.assertEqual(g["temp"], 61)
        self.assertEqual(g["pw"], 540.2)
        self.assertEqual(g["pw_lim"], 700.0)
        p = s["procs"][0]
        self.assertEqual(p["pid"], 1234)
        self.assertEqual(p["gpu"], 0)
        self.assertEqual(p["mem"], 40213)
        self.assertEqual(p["owner"], "yoonki")
        self.assertEqual(p["pod"], "yoonki-ume-xvrdr")
        self.assertEqual(p["started"], "2026-07-06T09:00:00Z")
        self.assertEqual(p["sm"], 85)

    def test_cmd_truncation(self):
        s = statsdb.snapshot_to_sample(make_snapshot())
        self.assertEqual(len(s["procs"][0]["cmd"]), 120)

    def test_none_passthrough(self):
        snap = make_snapshot()
        snap["gpus"][0]["util"] = None
        snap["gpus"][0]["temp_c"] = None
        snap["gpus"][0]["power_w"] = None
        snap["procs"][0]["owner"] = None
        snap["procs"][0]["sm_util"] = None
        snap["procs"][0]["mem_mib"] = None
        s = statsdb.snapshot_to_sample(snap)
        self.assertIsNone(s["gpus"][0]["util"])
        self.assertIsNone(s["gpus"][0]["temp"])
        self.assertIsNone(s["gpus"][0]["pw"])
        self.assertIsNone(s["procs"][0]["owner"])
        self.assertIsNone(s["procs"][0]["sm"])
        self.assertIsNone(s["procs"][0]["mem"])
        # None survives a JSON round trip as null.
        rt = json.loads(json.dumps(s))
        self.assertIsNone(rt["procs"][0]["owner"])

    def test_pods_key_present_when_ok(self):
        s = statsdb.snapshot_to_sample(make_snapshot(pods_ok=True))
        self.assertIn("pods", s)
        self.assertEqual(s["pods"][0]["pod"], "yoonki-ume-xvrdr")
        self.assertEqual(s["pods"][0]["req"], 4)
        self.assertEqual(s["pods"][0]["phase"], "Running")

    def test_pods_key_omitted_when_not_ok(self):
        s = statsdb.snapshot_to_sample(make_snapshot(pods_ok=False))
        self.assertNotIn("pods", s)

    def test_pods_key_omitted_when_absent(self):
        s = statsdb.snapshot_to_sample(make_snapshot(with_pods=False))
        self.assertNotIn("pods", s)


class TestSampleOnce(unittest.TestCase):
    def test_write_and_read_back(self):
        with tempfile.TemporaryDirectory() as d:
            fixed = datetime(2026, 7, 7, 4, 12, 30, tzinfo=timezone.utc)
            calls = {"n": 0}

            def collect(force=False):
                calls["n"] += 1
                self.assertTrue(force)
                return make_snapshot()

            path = statsdb._sample_once(collect, d, now_fn=lambda: fixed)
            self.assertTrue(path.endswith("samples-20260707.jsonl"))
            with open(path, "r", encoding="utf-8") as fh:
                lines = fh.read().splitlines()
            self.assertEqual(len(lines), 1)
            rec = json.loads(lines[0])
            self.assertEqual(rec["ts"], "2026-07-07T04:12:30Z")
            self.assertEqual(calls["n"], 1)

    def test_append_not_truncate(self):
        with tempfile.TemporaryDirectory() as d:
            fixed = datetime(2026, 7, 7, 4, 12, 30, tzinfo=timezone.utc)

            def collect(force=False):
                return make_snapshot()

            statsdb._sample_once(collect, d, now_fn=lambda: fixed)
            path = statsdb._sample_once(collect, d, now_fn=lambda: fixed)
            with open(path, "r", encoding="utf-8") as fh:
                lines = fh.read().splitlines()
            self.assertEqual(len(lines), 2)


class TestRotation(unittest.TestCase):
    def _write_raw_day(self, d, date_str, n=3):
        path = os.path.join(d, "samples-%s.jsonl" % date_str)
        with open(path, "w", encoding="utf-8") as fh:
            for i in range(n):
                ts = "%s-%s-%sT00:%02d:00Z" % (
                    date_str[:4], date_str[4:6], date_str[6:8], i)
                rec = statsdb.snapshot_to_sample(make_snapshot(ts=ts))
                fh.write(json.dumps(rec, separators=(",", ":")) + "\n")
        return path

    def test_rotate_gzips_and_rolls_up(self):
        with tempfile.TemporaryDirectory() as d:
            raw = self._write_raw_day(d, "20260706")
            now = datetime(2026, 7, 7, 0, 0, 5, tzinfo=timezone.utc)
            statsdb._rotate(d, "20260706", 365, 2048, now_fn=lambda: now)
            gz = os.path.join(d, "samples-20260706.jsonl.gz")
            self.assertTrue(os.path.isfile(gz))
            self.assertFalse(os.path.isfile(raw))
            rollup = os.path.join(d, "rollup-20260706.json")
            self.assertTrue(os.path.isfile(rollup))
            with open(rollup, "r", encoding="utf-8") as fh:
                r = json.load(fh)
            self.assertEqual(r["date"], "20260706")
            self.assertEqual(r["samples"], 3)

    def test_backfill_rotates_past_only(self):
        with tempfile.TemporaryDirectory() as d:
            self._write_raw_day(d, "20260705")
            today_raw = self._write_raw_day(d, "20260707")
            now = datetime(2026, 7, 7, 3, 0, 0, tzinfo=timezone.utc)
            statsdb._backfill(d, 365, 2048, now_fn=lambda: now)
            # Past day rotated.
            self.assertTrue(os.path.isfile(
                os.path.join(d, "samples-20260705.jsonl.gz")))
            self.assertFalse(os.path.isfile(
                os.path.join(d, "samples-20260705.jsonl")))
            # Today untouched.
            self.assertTrue(os.path.isfile(today_raw))
            self.assertFalse(os.path.isfile(
                os.path.join(d, "samples-20260707.jsonl.gz")))

    def test_size_cap_deletes_oldest_first_never_today(self):
        with tempfile.TemporaryDirectory() as d:
            # Today's raw file must never be deleted.
            today_raw = os.path.join(d, "samples-20260707.jsonl")
            with open(today_raw, "w", encoding="utf-8") as fh:
                fh.write("x\n")
            # Fake old .gz files, 1 MiB each, three days.
            payload = b"0" * (1024 * 1024)
            for date_str in ("20260701", "20260702", "20260703"):
                gz = os.path.join(d, "samples-%s.jsonl.gz" % date_str)
                with gzip.open(gz, "wb") as fh:
                    fh.write(payload)
                # gzip compresses zeros heavily; pad the on-disk file so the
                # size cap actually triggers. Re-open as raw and append filler.
                with open(gz, "ab") as fh:
                    fh.write(b"\x00" * (1024 * 1024))
            now = datetime(2026, 7, 7, 5, 0, 0, tzinfo=timezone.utc)
            # Cap of 2 MB with ~3 MB present -> delete oldest .gz first.
            statsdb._enforce_size_cap(d, 2, now_fn=lambda: now)
            self.assertFalse(os.path.isfile(
                os.path.join(d, "samples-20260701.jsonl.gz")),
                "oldest gz should be deleted first")
            # today's raw untouched
            self.assertTrue(os.path.isfile(today_raw))

    def test_size_cap_never_deletes_today_even_if_over(self):
        with tempfile.TemporaryDirectory() as d:
            today_raw = os.path.join(d, "samples-20260707.jsonl")
            with open(today_raw, "wb") as fh:
                fh.write(b"0" * (3 * 1024 * 1024))
            now = datetime(2026, 7, 7, 5, 0, 0, tzinfo=timezone.utc)
            statsdb._enforce_size_cap(d, 1, now_fn=lambda: now)
            self.assertTrue(os.path.isfile(today_raw))

    def test_retention_deletes_old_gz(self):
        with tempfile.TemporaryDirectory() as d:
            old = os.path.join(d, "samples-20250101.jsonl.gz")
            recent = os.path.join(d, "samples-20260706.jsonl.gz")
            for p in (old, recent):
                with gzip.open(p, "wb") as fh:
                    fh.write(b"{}\n")
            now = datetime(2026, 7, 7, 0, 0, 0, tzinfo=timezone.utc)
            statsdb._enforce_retention(d, 30, now_fn=lambda: now)
            self.assertFalse(os.path.isfile(old))
            self.assertTrue(os.path.isfile(recent))


class TestStatus(unittest.TestCase):
    def test_status_before_start(self):
        # status() must not blow up even if data_dir is None. We can't rely on
        # module state here (other tests may have started a sampler), so we
        # only assert the shape/keys.
        st = statsdb.status()
        for key in ("data_dir", "writable", "fallback", "files", "total_mb",
                    "sampling", "interval"):
            self.assertIn(key, st)


@unittest.skipIf(os.name == "nt", "read-only dir simulation is POSIX-only")
class TestFallback(unittest.TestCase):
    def test_unwritable_dir_falls_back(self):
        with tempfile.TemporaryDirectory() as d:
            # Use a file-as-dir: a regular file cannot host children.
            bogus = os.path.join(d, "not-a-dir")
            with open(bogus, "w", encoding="utf-8") as fh:
                fh.write("i am a file")
            resolved, fallback = statsdb._resolve_data_dir(bogus)
            self.assertTrue(fallback)
            self.assertNotEqual(resolved, bogus)


if __name__ == "__main__":
    unittest.main()
