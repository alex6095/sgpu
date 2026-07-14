"""Tests for the sgpu client's namespace resolution and --all logic.

The client itself only shells out to kubectl, so these cover the pure
decision logic: node shorthand expansion, the resolution precedence
(flag > env > context > fallback), and that --all is rejected for
interactive commands.
"""

import json
import os
import subprocess
import sys
import unittest
from io import BytesIO

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


class _FakeTTY:
    def __init__(self):
        self.buffer = []

    def write(self, text):
        self.buffer.append(text)

    def flush(self):
        pass

    def isatty(self):
        return True


class TestInteractiveReconnect(unittest.TestCase):
    """Fault-inject kubectl exec/pod states into the reconnect state machine."""

    def setUp(self):
        self._stdout, self._stdin, self._stderr = sys.stdout, sys.stdin, sys.stderr
        sys.stdout = _FakeTTY()
        sys.stdin = _FakeTTY()
        sys.stderr = _FakeTTY()
        self._exec = cli._interactive_exec
        self._pod_state_fn = cli._pod_state
        self._wait = cli._wait_for_ready_pod
        self._monitor_healthy = cli._monitor_healthy
        self._monotonic = cli.time.monotonic
        self._sleep = cli.time.sleep
        self._random = cli.random.random
        self._recovery_window = cli._RECONNECT_RECOVERY_WINDOW_SECONDS
        self._watchdog_stable = cli._WATCHDOG_STABLE_SESSION_SECONDS
        self._signal_settle = cli._SIGNAL_SETTLE_SECONDS
        self._signal_settle_delay = cli._SIGNAL_SETTLE_INITIAL_DELAY_SECONDS
        self.sleeps = []
        cli.time.sleep = lambda seconds: self.sleeps.append(seconds)
        cli.time.monotonic = lambda: 0
        cli.random.random = lambda: 0.5
        cli._wait_for_ready_pod = (
            lambda ns, pod, **kwargs: self._state("ready"))

    def tearDown(self):
        sys.stdout, sys.stdin, sys.stderr = self._stdout, self._stdin, self._stderr
        cli._interactive_exec = self._exec
        cli._pod_state = self._pod_state_fn
        cli._wait_for_ready_pod = self._wait
        cli._monitor_healthy = self._monitor_healthy
        cli.time.monotonic = self._monotonic
        cli.time.sleep = self._sleep
        cli.random.random = self._random
        cli._RECONNECT_RECOVERY_WINDOW_SECONDS = self._recovery_window
        cli._WATCHDOG_STABLE_SESSION_SECONDS = self._watchdog_stable
        cli._SIGNAL_SETTLE_SECONDS = self._signal_settle
        cli._SIGNAL_SETTLE_INITIAL_DELAY_SECONDS = self._signal_settle_delay

    @staticmethod
    def _state(uid="pod-a", ready=True, restart_count=0,
               monitor_image="image:old", container_id="container:old",
               started_at="2026-07-14T22:00:00Z"):
        return {
            "uid": uid,
            "phase": "Running" if ready else "Pending",
            "ready": ready,
            "restart_count": restart_count,
            "monitor_image": monitor_image,
            "container_id": container_id,
            "started_at": started_at,
            "error": None,
        }

    def _exec_results(self, results):
        calls = []
        results = iter(results)

        def fake_exec(command):
            calls.append(command)
            return next(results)

        cli._interactive_exec = fake_exec
        return calls

    def test_clean_exit_no_reconnect(self):
        cli._pod_state = lambda ns, pod: self._state()
        calls = self._exec_results([(0, "")])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 0)
        self.assertEqual(len(calls), 1)

    def test_rollout_replaces_pod_then_reconnects(self):
        states = iter([self._state("old"), self._state("new")])
        cli._pod_state = lambda ns, pod: next(states)
        calls = self._exec_results([
            (137, "command terminated with exit code 137\n"),
            (0, ""),
        ])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.sleeps, [1])
        written = "".join(sys.stdout.buffer)
        self.assertIn(cli._TERM_RESTORE, written)

    def test_fast_same_uid_restart_settles_until_new_container_state(self):
        """The first post-exec get can still show the old Ready container."""
        before = self._state(
            uid="same", restart_count=12, monitor_image="monitor:0.8.18",
            container_id="container:old", started_at="old-start")
        first_post = dict(before)
        settled = self._state(
            uid="same", restart_count=13, monitor_image="monitor:0.8.19",
            container_id="container:new", started_at="new-start")
        states = iter([before, first_post, settled])
        cli._pod_state = lambda ns, pod: next(states)
        calls = self._exec_results([
            (137, "command terminated with exit code 137\n"),
            (0, ""),
        ])
        self.assertEqual(
            cli._interactive("ns", "pod", ["python3", "tui.py"], False), 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.sleeps, [0.25, 1])
        self.assertIn("monitor pod changed", "".join(sys.stderr.buffer))

    def test_spec_change_waits_for_new_instance_then_recovers_exec_race(self):
        """Never exec into the old Ready container during an image rollout."""
        before = self._state(
            uid="same", restart_count=13, monitor_image="monitor:0.8.19",
            container_id="container:old", started_at="old-start")
        spec_changed_old_instance = self._state(
            uid="same", restart_count=13, monitor_image="monitor:0.8.20",
            container_id="container:old", started_at="old-start")
        container_absent = self._state(
            uid="same", ready=False, restart_count=13,
            monitor_image="monitor:0.8.20", container_id=None,
            started_at=None)
        new_instance = self._state(
            uid="same", restart_count=14, monitor_image="monitor:0.8.20",
            container_id="container:new", started_at="new-start")
        # First post-exec state has the new desired image but still reports
        # the old Ready container.  The real wait must reject it without
        # sending a /health exec to that container, then observe absence and
        # a new Ready instance.  The following exec can still race kubelet's
        # endpoint teardown once; classify that exact diagnostic as transient.
        states = iter([
            before,
            spec_changed_old_instance,
            spec_changed_old_instance,
            container_absent,
            new_instance,
            new_instance,
            new_instance,
        ])
        cli._pod_state = lambda ns, pod: next(states)
        health_checks = []
        cli._monitor_healthy = (
            lambda ns, pod: health_checks.append((ns, pod)) or True)
        cli._wait_for_ready_pod = self._wait
        calls = self._exec_results([
            (137, "command terminated with exit code 137\n"),
            (1, "error: Internal error occurred: unable to upgrade connection: "
                "container not found (\"monitor\")\n"),
            (0, ""),
        ])

        self.assertEqual(
            cli._interactive("ns", "pod", ["python3", "tui.py"], False), 0)
        self.assertEqual(len(calls), 3)
        self.assertEqual(health_checks, [("ns", "pod"), ("ns", "pod")])
        self.assertEqual(self.sleeps, [1, 2, 1, 2])
        self.assertIn("monitor pod changed", "".join(sys.stderr.buffer))
        self.assertNotIn("not reconnecting", "".join(sys.stderr.buffer))

    def test_unchanged_ready_pod_after_signal_fails_fast_after_settling(self):
        state = self._state(restart_count=12)
        clock = [0.0]
        cli._SIGNAL_SETTLE_SECONDS = 1
        cli._SIGNAL_SETTLE_INITIAL_DELAY_SECONDS = 0.5
        cli.time.monotonic = lambda: clock[0]

        def sleep(seconds):
            self.sleeps.append(seconds)
            clock[0] += seconds

        cli.time.sleep = sleep
        cli._pod_state = lambda ns, pod: state
        calls = self._exec_results([
            (137, "command terminated with exit code 137\n"),
        ])
        self.assertEqual(
            cli._interactive("ns", "pod", ["python3", "tui.py"], False), 137)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.sleeps, [0.5, 0.5])
        self.assertIn("lifecycle stayed unchanged",
                      "".join(sys.stderr.buffer))

    def test_sigterm_with_same_uid_restart_reconnects(self):
        before = self._state(
            uid="same", restart_count=12, container_id="container:old",
            started_at="old-start")
        after = self._state(
            uid="same", restart_count=13, container_id="container:new",
            started_at="new-start")
        states = iter([before, after])
        cli._pod_state = lambda ns, pod: next(states)
        calls = self._exec_results([
            (143, "command terminated with exit code 143\n"),
            (0, ""),
        ])
        self.assertEqual(
            cli._interactive("ns", "pod", ["python3", "tui.py"], False), 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.sleeps, [1])

    def test_signal_termination_recognizes_137_and_143_only(self):
        for code in (137, 143):
            with self.subTest(code=code):
                self.assertTrue(cli._is_remote_signal_termination(code, ""))
        self.assertTrue(cli._is_remote_signal_termination(
            1, "command terminated with exit code 143"))
        self.assertFalse(cli._is_remote_signal_termination(
            1, "command terminated with exit code 1"))

    def test_not_ready_pod_waits_before_reconnect(self):
        states = iter([self._state("old"), self._state("old", ready=False)])
        cli._pod_state = lambda ns, pod: next(states)
        cli._wait_for_ready_pod = (
            lambda ns, pod, **kwargs: self._state("new"))
        calls = self._exec_results([(137, ""), (0, "")])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.sleeps, [1])

    def test_remote_tui_exit_one_is_not_misreported_as_a_rollout(self):
        cli._pod_state = lambda ns, pod: self._state()
        calls = self._exec_results([
            (1, "command terminated with exit code 1\n"),
        ])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.sleeps, [])
        self.assertIn("not reconnecting", "".join(sys.stderr.buffer))

    def test_container_not_found_exec_race_is_narrowly_classified(self):
        self.assertTrue(cli._is_container_exec_race(
            "Internal error occurred: unable to upgrade connection: "
            "container not found (\"monitor\")"))
        self.assertFalse(cli._is_container_exec_race(
            'Error from server (NotFound): pods "pod" not found'))

    def test_auth_failure_does_not_retry_forever(self):
        cli._pod_state = lambda ns, pod: self._state()
        calls = self._exec_results([
            (1, "Error from server (Forbidden): pods/exec is forbidden\n"),
        ])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.sleeps, [])

    def test_pod_state_auth_failure_is_permanent_even_when_exec_has_no_stderr(self):
        forbidden = self._state(ready=False)
        forbidden["error"] = "Error from server (Forbidden): pods is forbidden"
        states = iter([self._state(), forbidden])
        cli._pod_state = lambda ns, pod: next(states)
        calls = self._exec_results([(1, "")])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 1)
        self.assertEqual(len(calls), 1)
        self.assertEqual(self.sleeps, [])
        self.assertIn("check your access", "".join(sys.stderr.buffer))

    def test_missing_pod_without_a_prior_lifecycle_is_not_retried(self):
        missing = self._state(uid=None, ready=False)
        missing["error"] = 'Error from server (NotFound): pods "pod" not found'
        cli._pod_state = lambda ns, pod: missing
        calls = self._exec_results([])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 1)
        self.assertEqual(len(calls), 0)
        self.assertEqual(self.sleeps, [])
        self.assertIn("cannot reach monitor pod", "".join(sys.stderr.buffer))

    def test_initial_rbac_failure_fails_before_launching_exec_child(self):
        denied = self._state(uid=None, ready=False)
        denied["error"] = "Error from server (Forbidden): pods is forbidden"
        cli._pod_state = lambda ns, pod: denied
        calls = self._exec_results([])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 1)
        self.assertEqual(calls, [])
        self.assertIn("check your access", "".join(sys.stderr.buffer))

    def test_tui_output_watchdog_exit_reconnects_as_transport_loss(self):
        cli._pod_state = lambda ns, pod: self._state()
        calls = self._exec_results([
            (75, "command terminated with exit code 75\n"),
            (0, ""),
        ])
        self.assertEqual(
            cli._interactive("ns", "pod", ["python3", "tui.py"], False), 0)
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.sleeps, [1])

    def test_long_lived_transport_losses_reset_the_failure_budget(self):
        # Six EOFs exceed the old process-lifetime limit of five. Each fake
        # session lasts one minute, so each has independently proved stable and
        # starts a fresh transient-failure budget.
        cli._pod_state = lambda ns, pod: self._state()
        clock = iter([0, 60, 60, 61, 121, 121, 122, 182, 182,
                      183, 243, 243, 244, 304, 304, 305, 365, 365,
                      366])
        cli.time.monotonic = lambda: next(clock)
        calls = self._exec_results(
            [(1, "unexpected EOF\n")] * 6 + [(0, "")])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 0)
        self.assertEqual(len(calls), 7)
        self.assertEqual(self.sleeps, [1] * 6)
        self.assertNotIn("giving up", "".join(sys.stderr.buffer))

    def test_unstable_transport_burst_uses_extended_bounded_recovery(self):
        cli._pod_state = lambda ns, pod: self._state()
        calls = self._exec_results(
            [(1, "unexpected EOF\n")] * 6 + [(0, "")])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 0)
        self.assertEqual(len(calls), 7)
        self.assertEqual(self.sleeps, [1, 2, 4, 8, 16, 16])

    def test_unstable_recovery_expires_at_its_window(self):
        cli._RECONNECT_RECOVERY_WINDOW_SECONDS = 1
        cli._pod_state = lambda ns, pod: self._state()
        clock = iter([0] * 16 + [1])
        cli.time.monotonic = lambda: next(clock)
        calls = self._exec_results([(1, "unexpected EOF\n")] * 6)
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 1)
        self.assertEqual(len(calls), 6)
        self.assertEqual(self.sleeps, [1, 2, 4, 8, 16])
        self.assertIn("recovery window expired", "".join(sys.stderr.buffer))

    def test_repeated_watchdog_exits_keep_the_original_recovery_deadline(self):
        """A 45s watchdog timeout must not look like a healthy 30s session."""
        clock = [0.0]
        self.sleeps = []
        cli.time.monotonic = lambda: clock[0]

        def sleep(seconds):
            self.sleeps.append(seconds)
            clock[0] += seconds

        def watchdog_exec(_command):
            # OUTPUT_STALL_SECONDS (45) is longer than the stable-session
            # cutoff.  Every recovery attempt dies in this exact same way.
            clock[0] += 45
            return 75, "command terminated with exit code 75\n"

        cli.time.sleep = sleep
        cli._pod_state = lambda ns, pod: self._state()
        cli._interactive_exec = watchdog_exec
        cli._wait_for_ready_pod = (
            lambda ns, pod, **kwargs: self._state("ready"))
        self.assertEqual(
            cli._interactive("ns", "pod", ["python3", "tui.py"], False),
            75)
        # Six reconnect sleeps occur before the seventh watchdog reaches the
        # first recovery window's five-minute deadline and is bounded out.
        self.assertEqual(self.sleeps, [1, 2, 4, 8, 16, 16])
        self.assertIn("recovery window expired", "".join(sys.stderr.buffer))

    def test_long_recovered_session_can_reset_before_watchdog_exit(self):
        """A long healthy run must not inherit an ancient recovery deadline."""
        clock = [0.0]
        timeouts = []
        cli._RECONNECT_RECOVERY_WINDOW_SECONDS = 10
        cli.time.monotonic = lambda: clock[0]

        def sleep(seconds):
            self.sleeps.append(seconds)
            clock[0] += seconds

        outcomes = iter([
            (1, "unexpected EOF\n", 0),
            (75, "command terminated with exit code 75\n", 120),
            (0, "", 0),
        ])

        def fake_exec(_command):
            code, stderr, elapsed = next(outcomes)
            clock[0] += elapsed
            return code, stderr

        def wait_ready(_ns, _pod, **kwargs):
            timeouts.append(kwargs["timeout"])
            return self._state() if kwargs["timeout"] > 0 else None

        cli.time.sleep = sleep
        cli._pod_state = lambda ns, pod: self._state()
        cli._interactive_exec = fake_exec
        cli._wait_for_ready_pod = wait_ready
        self.assertEqual(
            cli._interactive("ns", "pod", ["python3", "tui.py"], False),
            0)
        # The second 75 happened after two minutes of real output, so it gets
        # a new 10s recovery window rather than the elapsed first window.
        self.assertEqual(timeouts, [10, 10])
        self.assertEqual(self.sleeps, [1, 1])

    def test_ctrl_c_does_not_reconnect(self):
        cli._pod_state = lambda ns, pod: self._state()
        self._exec_results([(130, "")])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 130)

    def test_ctrl_c_while_waiting_for_readiness_stops_immediately(self):
        states = iter([self._state(), self._state(ready=False)])
        cli._pod_state = lambda ns, pod: next(states)

        def interrupted_wait(*args, **kwargs):
            raise KeyboardInterrupt()

        cli._wait_for_ready_pod = interrupted_wait
        self._exec_results([(1, "unexpected EOF\n")])
        self.assertEqual(cli._interactive("ns", "pod", ["x"], False), 130)
        self.assertEqual(self.sleeps, [])


class TestInteractiveExecProcess(unittest.TestCase):
    def test_fake_kubectl_like_process_surfaces_eof_diagnostic(self):
        """A real child process is used to reproduce kubectl's EOF exit path."""
        saved_stderr = sys.stderr
        fake_stderr = _FakeTTY()
        try:
            sys.stderr = fake_stderr
            code, diagnostic = cli._interactive_exec([
                sys.executable,
                "-c",
                "import sys; sys.stderr.write('unexpected EOF\\n'); sys.exit(1)",
            ])
        finally:
            sys.stderr = saved_stderr
        self.assertEqual(code, 1)
        self.assertEqual(diagnostic, "unexpected EOF\n")
        self.assertIn(diagnostic, "".join(fake_stderr.buffer))


class TestReconnectReadiness(unittest.TestCase):
    def setUp(self):
        self._state = cli._pod_state
        self._health = cli._monitor_healthy
        self._monotonic = cli.time.monotonic
        self._sleep = cli.time.sleep
        self._run = cli.subprocess.run

    def tearDown(self):
        cli._pod_state = self._state
        cli._monitor_healthy = self._health
        cli.time.monotonic = self._monotonic
        cli.time.sleep = self._sleep
        cli.subprocess.run = self._run

    @staticmethod
    def _ready_state():
        return {"uid": "pod", "phase": "Running", "ready": True,
                "restart_count": 0, "error": None}

    def test_ready_pod_waits_for_health_with_bounded_probe_spacing(self):
        now = [0.0]
        sleeps = []
        health = iter([False, True])
        cli._pod_state = lambda ns, pod: self._ready_state()
        cli._monitor_healthy = lambda ns, pod: next(health)
        cli.time.monotonic = lambda: now[0]

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        cli.time.sleep = sleep
        self.assertEqual(
            cli._wait_for_ready_pod("ns", "pod", require_health=True)["uid"],
            "pod")
        self.assertEqual(sleeps, [1])

    def test_health_outage_can_recover_beyond_the_old_sixty_second_wait(self):
        now = [0.0]
        sleeps = []
        cli._pod_state = lambda ns, pod: self._ready_state()
        cli._monitor_healthy = lambda ns, pod: False
        cli.time.monotonic = lambda: now[0]

        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds

        cli.time.sleep = sleep
        self.assertGreater(cli._RECONNECT_RECOVERY_WINDOW_SECONDS, 60)
        self.assertIsNone(
            cli._wait_for_ready_pod("ns", "pod", require_health=True,
                                    timeout=61))
        self.assertEqual(sleeps, [1, 2, 4, 8, 16, 16, 14])

    def test_fake_kubectl_reports_uid_ready_restart_and_health(self):
        pod_json = json.dumps({
            "metadata": {"uid": "pod-uid"},
            "spec": {"containers": [{
                "name": "monitor",
                "image": "docker.io/alex6095/sgpu-monitor:0.8.19",
            }]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
                "containerStatuses": [{
                    "name": "monitor",
                    "restartCount": 12,
                    "containerID": "containerd://new",
                    "state": {"running": {
                        "startedAt": "2026-07-14T22:32:30Z",
                    }},
                }],
            },
        })
        calls = []

        def fake_kubectl(command, **kwargs):
            calls.append(command)
            if command[1:3] == ["get", "pod"]:
                return subprocess.CompletedProcess(command, 0, stdout=pod_json,
                                                   stderr="")
            return subprocess.CompletedProcess(command, 0, stdout="503",
                                               stderr="")

        cli.subprocess.run = fake_kubectl
        state = cli._pod_state("ns", "pod")
        self.assertEqual((state["uid"], state["ready"], state["restart_count"]),
                         ("pod-uid", True, 12))
        self.assertEqual(state["monitor_image"],
                         "docker.io/alex6095/sgpu-monitor:0.8.19")
        self.assertEqual(state["container_id"], "containerd://new")
        self.assertEqual(state["started_at"], "2026-07-14T22:32:30Z")
        self.assertTrue(cli._monitor_healthy("ns", "pod"))
        self.assertIn("curl", calls[-1])

    def test_backoff_jitter_is_bounded(self):
        self.assertEqual(cli._reconnect_delay(1, random_value=0), 0.8)
        self.assertEqual(cli._reconnect_delay(1, random_value=1), 1.2)
        self.assertEqual(cli._reconnect_delay(9, random_value=0), 12.8)
        self.assertEqual(cli._reconnect_delay(9, random_value=1), 16)


class TestVersionCompare(unittest.TestCase):
    def test_outdated(self):
        self.assertTrue(cli._is_outdated("0.7.0", "0.8.3"))
        self.assertTrue(cli._is_outdated("0.8.2", "0.8.3"))

    def test_current_or_ahead(self):
        self.assertFalse(cli._is_outdated("0.8.3", "0.8.3"))
        self.assertFalse(cli._is_outdated("0.9.0", "0.8.3"))

    def test_missing_versions(self):
        self.assertFalse(cli._is_outdated("0.8.3", None))
        self.assertFalse(cli._is_outdated(None, "0.8.3"))

    def test_version_tuple(self):
        self.assertEqual(cli._version_tuple("0.8.3"), (0, 8, 3))
        self.assertEqual(cli._version_tuple("1.2.3rc1"), (1, 2, 3))


class _FakePipe:
    def __init__(self):
        self.buffer = BytesIO()

    def isatty(self):
        return False

    def write(self, text):
        self.buffer.write(text.encode("utf-8"))

    def flush(self):
        pass


class TestAgentJson(unittest.TestCase):
    def test_print_pipe_has_no_utf8_bom(self):
        saved = sys.stdout
        fake = _FakePipe()
        try:
            sys.stdout = fake
            cli._print('{"ok": true}')
        finally:
            sys.stdout = saved
        raw = fake.buffer.getvalue()
        self.assertTrue(raw.startswith(b"{"))
        self.assertNotEqual(raw[:3], b"\xef\xbb\xbf")

    def test_agent_snapshot_has_stable_v1_shape(self):
        snap = {
            "schema": 2,
            "source": "nvml",
            "time_utc": "2026-07-09T00:00:00Z",
            "node": "h200-04-w-4b11",
            "driver": "580.126.16",
            "sgpu_version": "0.8.14",
            "gpus": [{"index": 0, "util": 90}],
            "procs": [{"pid": 7, "gpu_index": 0, "owner": "ty",
                       "pod": "ty-job", "cmd": "python train.py"}],
            "pods": {"ok": True, "source": "kube", "rows": [{
                "owner": "ty", "pod": "ty-job", "phase": "Running",
                "gpu": 1, "active": 1, "uid": "u"}]},
            "gpu_free": {"free": 0, "total": 8},
        }
        out = cli._agent_snapshot(snap, "p-sgvr-node-02")
        self.assertEqual(out["agent_schema"], 1)
        self.assertEqual(out["kind"], "snapshot")
        self.assertEqual(out["node"]["label"], "2")
        self.assertEqual(out["raw_schema"], 2)
        self.assertEqual(out["processes"][0]["cmd"], "python train.py")
        self.assertEqual(out["pods"][0]["request"], 1)

    def test_agent_stats_scope_mapping(self):
        self.assertEqual(cli._json_scope("lab", "p-sgvr-node-02"), "lab")
        self.assertEqual(cli._json_scope("1", "p-sgvr-node-02"), "1")
        self.assertEqual(cli._json_scope(None, "p-sgvr-node-02"), "local")
