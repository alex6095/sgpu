# Changelog

All notable changes to **sgpu**, newest first. Versions are lockstepped: the
PyPI client and the `docker.io/alex6095/sgpu-monitor` server image share one
number so a client never falsely reports itself "behind the server".

## 0.8.21 - Instance-ready rollout reconnect hotfix

- Fixed the remaining in-place rollout race: a patched monitor image can be
  visible while Ready and health still belong to the old container.  A
  spec-only image change now waits, within the existing bounded recovery
  window, for a baseline-relative monitor instance transition before a new
  exec is opened.
- Require that transitioned instance to be Ready and pass its health check;
  classify only kubectl's named-container-not-found upgrade diagnostic as the
  short-lived exec race it is, while preserving fail-fast handling for pod
  NotFound, RBAC, and same-container application exits.

---

## 0.8.20 - Rollout reconnect hotfix

- Fixed a release-blocking race during an in-place monitor container rollout:
  an active kubectl exec can end with exit 137/143 while its first post-exit
  get-pod probe still reports the old Ready container.
- Capture monitor image, container ID, started time, and restart count before
  exec; after a signal termination, perform a short bounded settling re-probe
  and reconnect only when concrete same-UID container lifecycle evidence
  appears.
- Keep an unchanged Ready pod after the settling window fail-fast, preserving
  protection against remote application failures while retaining bounded EOF,
  transport, watchdog, and rollout recovery.

---

## 0.8.19 - Self-healing sessions and cluster pulse

- Fixed the real long-session failure mode: a disconnected `kubectl exec`
  stream could leave its remote pseudo-terminal and TUI alive indefinitely,
  continuing to poll the monitor after the local client had already exited.
- Added a byte-progress watchdog that terminates an orphaned remote TUI after
  sustained output blockage while still tolerating short backpressure.
- Replaced the lifetime five-retry counter with pod UID/Ready/restart/health
  checks, permanent-vs-transient error classification, a resettable failure
  budget, and bounded exponential recovery with jitter.
- Added a terminal-native Cluster pulse with KST utilization/VRAM sparklines,
  utilization zones, observed compute-window cadence, and owner Momentum.
- Introduced rollup schema v2 using compact weighted telemetry accumulators;
  retained v1 history upgrades lazily and atomically when raw data is present,
  with safe missing-data and concurrent-request behavior.
- Added deterministic transport, real unread-PTY, metric, narrow/ASCII render,
  migration, read-only, and concurrent-upgrade regression tests.
- Kept repeated default 45-second unread-PTY watchdog exits inside one bounded
  recovery window, while allowing a genuinely long recovered session to start
  a fresh budget; initial RBAC and missing-pod probes now fail before `exec`.
- Stitched Flow across UTC daily boundaries; LAB output explicitly reports the
  exact sum of node compute windows and longest per-node window rather than a
  fictional cross-node union, and marks partial telemetry by node-day.
- Hardened retained raw telemetry against NaN/null device and process values.
- Made daily raw rollups adapt to confirmed sustained sampler cadence changes
  (including 15/16s to 60/61s) without changing the compatible daily median;
  normal slower samples retain full credit and one Flow window, while missing
  telemetry still has bounded credit and breaks Flow.

---

## 0.8.18 - TUI output backpressure resilience

- Fixed long-running TUI freezes caused by ncurses blocking in `write(2)`
  when a `kubectl exec` pseudo-terminal temporarily stopped draining output.
- Buffered complete curses frames away from the remote PTY, forwarded them
  nonblockingly with bounded retry backoff, and forced a full repaint after
  recovery without truncating terminal escape sequences.
- Added an independent zero-timeout input poll so terminal state churn cannot
  put the UI thread back into a blocking `getch()` call.
- Added a real POSIX pseudo-terminal regression test that fills an unread PTY
  and verifies the curses loop remains live instead of stalling.

---

## 0.8.17 - TUI startup fix

- Fixed a TUI startup crash in the 0.8.16 owner-color palette registration
  path (`'list' object has no attribute 'items'`), so plain `sgpu` opens the
  interactive dashboard again instead of falling back to text output.
- Added a curses-attribute regression test that exercises the 20 owner-color
  tags during TUI initialization.

---

## 0.8.16 - Owner color palette

- Expanded owner colors from 6 slots to a 20-color palette for lab-scale
  dashboards.
- Added per-screen owner color assignment so visible owners avoid collisions
  when the palette has room, while keeping the mapping deterministic for the
  same owner set.
- Applied the shared owner color map across the dashboard, stats screen,
  detail screen, and text renderers so owner names stay visually consistent.

---

## 0.8.15 — Agent JSON and nonblocking TUI input

- Added stable agent-oriented JSON output with `agent_schema: 1` via
  `--json` on text commands such as `json`, `pods`, `apps`, `stats`,
  `health`, and `version`; failures are JSON too where possible.
- Fixed Windows direct stdout capture by avoiding UTF-8 BOM emission from the
  client itself. Note: Windows PowerShell's native-to-native pipe can still
  inject a BOM; direct process capture and `cmd` pipes are clean.
- Fixed a long-running TUI freeze: the remote curses main thread could lose
  its timeout and block forever in `getch()`/TTY read while background fetches
  kept running. Input is now fully nonblocking with explicit 20 Hz pacing.

---

## 0.8.14 — Help clarity and wrapped command color

- Help now wraps every long explanation to the terminal width instead of
  clipping the right side, and its key hints match the current node/scope
  model (`n node 1/2`, `n scope 1/2/LAB`).
- Help sections now have visual dividers and colored labels so keys, metrics,
  and awards are easier to scan in a wide terminal.
- Detail views keep wrapped related-process command lines in the same color as
  the first line, instead of turning continuation lines dim gray.

---

## 0.8.13 — Node switching and detail polish

- The interactive dashboard can now switch nodes in-place with `n`, so you can
  jump between node 1 and node 2 without quitting and relaunching `sgpu -n 1`
  or `sgpu -n 2`.
- The stats screen scope now follows the node model: opening from node 2 cycles
  `scope:2 -> scope:1 -> scope:LAB` (and vice versa from node 1), instead of
  the less-specific `LOCAL` / `LAB` toggle.
- Footer hints now shrink by whole command chunks, so narrow terminals do not
  cut words like `refresh` in half.
- Detail screens prefer arrow/wheel scroll hints and wrap long related-process
  commands instead of clipping the command just when you opened detail to read
  it.

---

## 0.8.12 — Pod table fills the screen

- The pods pane now **expands with the terminal height** like the process
  table: once processes fit, spare height grows the pod list until every pod
  is visible, instead of stopping at a fixed 6-row cap. The `… N more` hint
  only appears when the list genuinely doesn't fit. Processes still own the
  main flexible area, and pods keep a small minimum so they never vanish on a
  medium screen.

---

## 0.8.11 — Row detail panel, "N more" pods, snappy Esc

- **Enter opens a detail panel** for the selected process or pod: owner, pod,
  PID, GPU (index/UUID), live SM% / memory / uptime, full command, the GPU's
  state, the Kubernetes pod fields, and the pod's other GPU processes. It
  follows the live snapshot; Enter or Esc returns; `j/k`/PgUp/PgDn/wheel
  scroll. Stale selections show the last known values.
- The pods pane now shows a **`… N more (Tab+scroll)`** hint when the list is
  truncated — fixes a case where a pod (e.g. `ty-lpwm-panda2t`) was pushed
  below the 6-row cap by alphabetically-earlier owners and looked missing (it
  was collected correctly all along; only the display truncated silently).
- **Esc is now instant** (ncurses `ESCDELAY` lowered from ~1s), so leaving the
  help / stats / detail screens no longer lags.

---

## 0.8.10 — Live clock, index alignment, changelog

- **Live header clock**: the time now ticks every second (a real wall clock)
  instead of jumping every ~2s with the data refresh; the `(age Ns)` counter
  still shows data staleness.
- GPU **index sits under the `G`** of "GPU" (was under the `U`) for a steadier
  read.
- Added this **CHANGELOG.md**.

---

## 0.8.9 — Smooth spinners, faster spin, arrow hints

- **Fixed the intermittent freeze** (screen stuck until you scrolled): it was
  the `kubectl exec` stream stalling, made worse by the 10 Hz full-screen
  redraws the spinner introduced. Animation ticks now **repaint only the ~8
  spinner cells in place** (~1 KB/s over the stream vs tens of KB/s), doing a
  full rebuild only on data/key/resize. The stats screen is gated the same way.
- **Retuned spin speed** at 20 Hz: idle is static, a ~10% card visibly turns,
  a 100% card spins fast — a clear fan-like gradient.
- Fixed the spinner gutter to 2 chars (had shifted GPU rows one column off).
- Footer scroll hint `j/k` → `↑↓` (mouse / arrows / j-k all still work).

---

## 0.8.8 — Animated GPU work spinners

- Each active GPU row leads with a **braille rotation spinner whose speed
  tracks util** — a card grinding at 100% spins fast, one coasting turns
  slowly, idle shows a static dot. Reads like a fan at a glance.
- The stats **loading** state gets a distinct sparkle pulse (`✶✳✻✽`).
- Text report gains a static activity dot in the same gutter.

---

## 0.8.7 — Heatmap legend spacing

- A blank line separates the KST heatmap / grass grid from its `less…more`
  legend in both the TUI and the text report (they had blurred together).

---

## Operational — 60s sampling *(2026-07-08)*

- Stats sampling interval **15s → 60s** (env only, no version bump). Measured
  ~0.4 MB/day/node at 15s, so storage was never the constraint; GPU jobs run
  for hours, so 15s oversampled. 60s keeps 1440 samples/day — util averages,
  KST heatmap and "idle now" detection stay robust. Per-day interval inference
  means old 15s data and new 60s data aggregate correctly; no pruning needed.

---

## 0.8.6 — Install-aware upgrade nudge

- The "update available" nudge now **detects how the client was installed**
  (uv tool / pipx / pip) and shows the matching upgrade command — no more
  telling a uv user to run a pip command they don't have. The TUI banner honors
  the command the client passes via `SGPU_UPGRADE_CMD`.

---

## 0.8.5 — Always-visible version, layout tidy-up

- Header **always shows the version** (`SGPU vX.Y.Z`), so you know what you're
  on even when current.
- The `[N/M free +K idle]` badge moved from the header to its own line under
  the GPU table (with a "K in use" tail).
- Full-width **section dividers** in the dashboard and stats report; the text
  report unifies storage under the GPUs.

---

## 0.8.4 — Upgrade nudge

- When the client falls behind the server, the TUI shows a yellow
  `↑ update available` banner and text commands print a one-line hint. The
  server version is cached 6h so it costs no extra round trip per command;
  `SGPU_NO_UPDATE_CHECK=1` silences it.

---

## 0.8.3 — Idle-reserved GPUs in the badge

- The free badge shows `[1/8 free +1 idle]`: `N` = requestable now, `+K idle`
  = GPUs reserved by Running pods that aren't using them (reclaimable). Reveals
  the case where the cluster is physically freer than the scheduler thinks.

---

## 0.8.2 — Free-GPU headline badge

- A header badge shows how many GPUs a **new pod could request right now**
  (total minus pods' GPU requests) — green when free, red when full.

---

## 0.8.1 — Green stats grid, ranked leaderboard

- The TUI stats grid renders in the same **GitHub-green scale** as the text
  report (it fell back to monochrome because `kubectl exec` lands `TERM=xterm`;
  now upgraded to a 256-color TERM, with a green fallback for 8-color terms).
- Leaderboards show `1. 2. 3.` **rank numbers** (TUI and text).

---

## 0.8.0 — Lab-wide merged stats

- `sgpu --all stats` (and the TUI stats screen's `n` key) produce **one
  lab-wide report** merging both nodes: combined leaderboard, awards and
  heatmaps, plus a **NODE column** showing each person's home node (or `both`).
  Any reachable monitor pulls its peers' `/stats` over the cluster network and
  renders the merge server-side.

---

## 0.7.3 — TUI auto-reconnect

- When a deploy recreates the monitor pod, the open TUI's `kubectl exec` is
  SIGKILLed (exit 137). The client now **restores the terminal** (mouse off,
  cursor on, leave alt screen) and **reconnects** once the pod is Ready, so an
  update looks like a 1–2s blip. Ctrl-C / clean quit never reconnect.

---

## 0.7.2 — In-TUI help overlay

- Press `?` in the TUI for a scrollable help overlay: key reference, a metrics
  glossary (UTIL vs SM%, GPU-H, EFF-H, ALLOC/IDLE, …) and every award's exact
  criteria. README gains matching "What the numbers mean" and "Awards" tables.

---

## 0.7.1 — Streaming stats aggregation

- `/stats` aggregates raw JSONL **line-by-line in constant memory** (was
  materializing a whole day into a list), with a short-TTL query cache and a
  streaming `/stats/raw` response — removes the read-side memory peak.

---

## 0.7.0 — Multi-node + public image

- **Two nodes** (`p-sgvr-node-01/-02`): pick one with `-n 1` / `-n 2`, survey
  both with `--all`; namespace resolves flag > env > current kubectl context.
- Monitor runs from a **public Docker Hub image** — no NCR login or pull
  secret; one manifest deploys to any node via `kubectl apply -n <ns>`.

---

## 0.6.0 — First public release

- **Single cross-platform Python client** on PyPI (`uvx` / `pipx` / `pip`);
  the duplicated bash + PowerShell clients were removed.
- **Stats report overhaul**: emoji **awards**, GitHub-style **grass calendar**,
  full owner names; block-cell KST heatmap.
- **Interactive TUI stats screen** with hour / day / week / month axes.
- **Shared-storage (pv-01/pv-02) usage** shown in the dashboard and TUI.
- Stats persist on the shared PVC (`pv-01/sangmin/sgpu`).

---

## 0.5.0 — Foundation *(unreleased)*

- **Server-side rendering**: the monitor pod renders the dashboard, so any
  `kubectl exec … curl` gets the same output; ANSI color is opt-in.
- **In-pod curses TUI** over `kubectl exec -it` — the refresh loop runs in the
  pod, so remote clients stay smooth.
- **Process → pod/owner attribution** via `/proc/<pid>/environ` (hostPID),
  with a cgroup-UID fallback.
- **Per-owner usage accounting**: 15s NVML samples → daily JSONL (full raw
  fidelity) → gzip + rollups, retention + size cap.
- Port-forward path removed; thin `kubectl exec` clients only.
