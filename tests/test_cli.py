"""Tests for the sgpu client's namespace resolution and --all logic.

The client itself only shells out to kubectl, so these cover the pure
decision logic: node shorthand expansion, the resolution precedence
(flag > env > context > fallback), and that --all is rejected for
interactive commands.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sgpu"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sgpu import cli


class TestExpandNs(unittest.TestCase):
    def test_node01_shorthands(self):
        for value in ("1", "01", "node-1", "node-01", "NODE-01", " 1 "):
            self.assertEqual(cli._expand_ns(value), "p-sgvr-node-01")

    def test_node02_shorthands(self):
        for value in ("2", "02", "node-2", "node-02"):
            self.assertEqual(cli._expand_ns(value), "p-sgvr-node-02")

    def test_full_namespace_passthrough(self):
        self.assertEqual(cli._expand_ns("p-sgvr-node-01"), "p-sgvr-node-01")
        self.assertEqual(cli._expand_ns("some-other-ns"), "some-other-ns")

    def test_none(self):
        self.assertIsNone(cli._expand_ns(None))


class TestResolveNamespace(unittest.TestCase):
    def setUp(self):
        self._saved_env = os.environ.get("SGPU_NAMESPACE")
        os.environ.pop("SGPU_NAMESPACE", None)
        self._saved_ctx = cli._context_ns

    def tearDown(self):
        if self._saved_env is None:
            os.environ.pop("SGPU_NAMESPACE", None)
        else:
            os.environ["SGPU_NAMESPACE"] = self._saved_env
        cli._context_ns = self._saved_ctx

    def test_explicit_flag_wins_and_expands(self):
        os.environ["SGPU_NAMESPACE"] = "p-sgvr-node-02"
        cli._context_ns = lambda: "ctx-ns"
        self.assertEqual(cli._resolve_namespace("1"), "p-sgvr-node-01")

    def test_env_used_when_no_flag(self):
        os.environ["SGPU_NAMESPACE"] = "2"
        cli._context_ns = lambda: "ctx-ns"
        self.assertEqual(cli._resolve_namespace(None), "p-sgvr-node-02")

    def test_context_used_when_no_flag_or_env(self):
        cli._context_ns = lambda: "p-sgvr-node-01"
        self.assertEqual(cli._resolve_namespace(None), "p-sgvr-node-01")

    def test_fallback_when_nothing_set(self):
        cli._context_ns = lambda: None
        self.assertEqual(cli._resolve_namespace(None), cli.FALLBACK_NAMESPACE)


class TestAllRejectsInteractive(unittest.TestCase):
    def setUp(self):
        self._saved_which = cli.shutil.which
        cli.shutil.which = lambda name: "/usr/bin/kubectl"

    def tearDown(self):
        cli.shutil.which = self._saved_which

    def test_all_with_top_errors(self):
        self.assertEqual(cli.main(["top", "--all"]), 2)

    def test_all_with_nvitop_errors(self):
        self.assertEqual(cli.main(["nvitop", "--all"]), 2)

    def test_all_with_watch_errors(self):
        self.assertEqual(cli.main(["watch", "--all"]), 2)


if __name__ == "__main__":
    unittest.main()
