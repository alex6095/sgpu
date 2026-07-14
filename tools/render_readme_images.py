"""Render high-quality README images for SGPU.

The generated files are SVGs, so they stay crisp on GitHub at any zoom level.
Samples are synthetic and anonymized by default. Set SGPU_README_LIVE=1 to
capture the current monitor output through the local sgpu client before
anonymizing it.
"""

from __future__ import annotations

import html
import os
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "images"
ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
FAKE_OWNERS = ["atlas", "nova", "orion", "vega", "lyra", "mira"]
FAKE_PODS = [
    "atlas-vla-train-a",
    "nova-world-model-b",
    "orion-policy-eval",
    "vega-dataset-cache",
    "lyra-debug-shell",
    "mira-render-job",
]
FAKE_CMDS = [
    "python vla.py",
    "python wm.py",
    "python eval.py",
    "python cache.py",
    "python nb.py",
    "python render.py",
]


SAMPLE_DASHBOARD = """SGPU  node=h200-04-w-4b11  driver=580.126.16  23:07:15 UTC (08:07 KST)

GPU NAME      UTIL          MEM                    TEMP      POWER  OWNERS
  0 H200      [########--]   78%  [######----] 78.6G/140.4G   55C   416/700W  atlas
  1 H200      [----------]    0%  [----------]  0.6G/140.4G   31C    77/700W  -
  2 H200      [----------]    0%  [----------]  0.6G/140.4G   30C    75/700W  -
  3 H200      [##########]   96%  [####------] 61.6G/140.4G   60C   548/700W  nova
  4 H200      [######----]   64%  [#####-----] 73.8G/140.4G   63C   363/700W  atlas
  5 H200      [########--]   82%  [#####-----] 70.7G/140.4G   52C   401/700W  atlas
  6 H200      [########--]   75%  [######----] 81.8G/140.4G   53C   408/700W  atlas
  7 H200      [----------]    0%  [----------]  0.6G/140.4G   30C    77/700W  -

NVIDIA compute processes
GPU  OWNER     POD                                             PID   SM%      MEM       UP  CMD
  0  atlas     atlas-vla-train-a                             12034    74    77.8G   21h38m  python vla.py
  3  nova      nova-world-model-b                            12035    96    61.0G    1d20h  python wm.py
  4  atlas     atlas-vla-train-a                             12036    71    73.1G   21h38m  python eval.py
  5  atlas     atlas-vla-train-a                             12037    78    70.0G   21h38m  python cache.py
  6  atlas     atlas-vla-train-a                             12038    79    81.1G   21h38m  python nb.py

Kubernetes GPU pods on this node
OWNER     REQ  ACT  AGE      PHASE     POD
nova        1    1  1d20h    Running   nova-world-model-b
atlas       4    4  21h38m   Running   atlas-vla-train-a

STORAGE pv-01/pv-02 [########--] 83.0%  34.8T/41.9T used, 7.1T free
"""


SAMPLE_MULTI_NODE = """=== p-sgvr-node-01 ===
SGPU  node=h200-04-w-495c  driver=580.126.16  23:07:16 UTC (08:07 KST)

GPU NAME      UTIL          MEM                    TEMP      POWER  OWNERS
  0 H200      [----------]    0%  [----------]  0.6G/140.4G   31C    77/700W  -
  1 H200      [#########-]   92%  [#####-----] 65.2G/140.4G   58C   522/700W  orion
  2 H200      [----------]    0%  [----------]  0.6G/140.4G   30C    76/700W  -
  3 H200      [----------]    0%  [----------]  0.6G/140.4G   30C    75/700W  -
  4 H200      [########--]   81%  [#####-----] 66.0G/140.4G   54C   462/700W  orion
  5 H200      [----------]    0%  [----------]  0.6G/140.4G   31C    78/700W  -
  6 H200      [----------]    0%  [----------]  0.6G/140.4G   31C    78/700W  -
  7 H200      [----------]    0%  [----------]  0.6G/140.4G   30C    77/700W  -

=== p-sgvr-node-02 ===
SGPU  node=h200-04-w-4b11  driver=580.126.16  23:07:17 UTC (08:07 KST)

GPU NAME      UTIL          MEM                    TEMP      POWER  OWNERS
  0 H200      [########--]   78%  [######----] 78.6G/140.4G   55C   416/700W  atlas
  1 H200      [----------]    0%  [----------]  0.6G/140.4G   31C    77/700W  -
  2 H200      [----------]    0%  [----------]  0.6G/140.4G   30C    75/700W  -
  3 H200      [##########]   96%  [####------] 61.6G/140.4G   60C   548/700W  nova
  4 H200      [######----]   64%  [#####-----] 73.8G/140.4G   63C   363/700W  atlas
  5 H200      [########--]   82%  [#####-----] 70.7G/140.4G   52C   401/700W  atlas
  6 H200      [########--]   75%  [######----] 81.8G/140.4G   53C   408/700W  atlas
  7 H200      [----------]    0%  [----------]  0.6G/140.4G   30C    77/700W  -
"""


SAMPLE_APPS = """NVIDIA compute processes
GPU  OWNER     POD                                             PID   SM%      MEM       UP  CMD
  0  atlas     atlas-vla-train-a                             12034    74    77.8G   21h38m  python vla.py
  3  nova      nova-world-model-b                            12035    96    61.0G    1d20h  python wm.py
  4  atlas     atlas-vla-train-a                             12036    71    73.1G   21h38m  python eval.py
  5  atlas     atlas-vla-train-a                             12037    78    70.0G   21h38m  python cache.py
  6  atlas     atlas-vla-train-a                             12038    79    81.1G   21h38m  python nb.py
"""


SAMPLE_STATS = """SGPU usage report - last 30 days - all nodes (node-01+node-02)
data: 7 days, coverage 168.0h

Cluster pulse
KST  UTIL ▁▁▂▃▄▆▇█▇▆▄▃▂▂▃▄▆▇▆▄▃▂▁▁  avg 54%  hot 31%
     VRAM ▁▂▂▃▄▅▆▇▇▆▅▄▃▃▄▅▆▇▇▆▄▃▂▁  avg 47%
UTIL mix  quiet 23%  light 18%  work 28%  hot 31%
Flow      12 node compute windows - longest 9h18m per-node (any GPU >= 50%)

Awards
* Best researcher: atlas - 123.4 effective GPU-h (76% avg over 162.4 GPU-h)
* Power user: orion - 188.0 GPU-h
* Sharpshooter: nova - 91% avg SM over 64.8 GPU-h

Leaderboard
OWNER     GPU-H  EFF-H  AVG-SM%  AVG-UTIL%  PEAK-MEM  ALLOC-H   IDLE-H  IDLE%
orion     188.0  132.5       70         73      91.2    188.0      3.2      2
atlas     162.4  123.4       76         78      81.1    162.4      0.0      0
nova       64.8   59.0       91         94      61.0     64.8      0.0      0

Momentum
atlas 7d streak - 7/7d - 76% eff  |  nova 5d streak - 6/7d - 91% eff

KST hour activity (gpu-seconds share)
KST      0     3     6     9     12    15    18    21
orion    ..::--==++**##%%##**++==--::..
atlas       ..::--==++**##%%##**++==..
nova              ..::--==++**##%%##..
TOTAL    ..::--==++**##%%##%%##**++==..
"""


def strip_ansi(text: str) -> str:
    text = ANSI_RE.sub("", text)
    return text.replace("\r\n", "\n").replace("\r", "\n").strip("\n")


def capture(args: list[str], fallback: str) -> str:
    if os.environ.get("SGPU_README_LIVE") != "1":
        return fallback
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        proc = subprocess.run(
            args,
            cwd=ROOT,
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=25,
        )
    except Exception:
        return fallback
    if proc.returncode != 0 or not proc.stdout.strip():
        return fallback
    return proc.stdout


def discover_private_tokens(text: str) -> tuple[dict[str, str], dict[str, str]]:
    owners: list[str] = []
    pods: list[str] = []

    def add_owner(value: str) -> None:
        value = value.strip()
        if not value or value in {"-", "?", "OWNER", "OWNERS", "TOTAL", "Running", "Pending"}:
            return
        if value not in owners:
            owners.append(value)

    def add_pod(value: str) -> None:
        value = value.strip()
        if not value or value in {"-", "?", "POD"}:
            return
        if value not in pods:
            pods.append(value)

    for line in strip_ansi(text).splitlines():
        if re.match(r"^\s*\d+\s+H200\b", line):
            tail = line.rsplit(None, 1)[-1]
            for owner in tail.split(","):
                add_owner(owner)
        proc = re.match(r"^\s*\d+\s+(\S+)\s+(\S+)\s+\d+\s+", line)
        if proc:
            add_owner(proc.group(1))
            add_pod(proc.group(2))
            continue
        pod_row = re.match(r"^(\S+)\s+\d+\s+\d+\s+\S+\s+(?:Running|Pending)\s+(\S+)", line)
        if pod_row:
            add_owner(pod_row.group(1))
            add_pod(pod_row.group(2))
            continue
        stat_row = re.match(r"^(\S+)\s+\d+(?:\.\d+)?\s+\d", line)
        if stat_row and not line.startswith(("GPU ", "OWNER", "KST")):
            add_owner(stat_row.group(1))
        award = re.search(r":\s+([A-Za-z0-9_.-]+)\s+-", line)
        if award:
            add_owner(award.group(1))
        heatmap = re.match(r"^([A-Za-z0-9_.-]+)\s+[.#:\-=+*% ]+$", line)
        if heatmap:
            add_owner(heatmap.group(1))

    owner_map = {
        owner: FAKE_OWNERS[index % len(FAKE_OWNERS)]
        for index, owner in enumerate(owners)
    }
    pod_map = {
        pod: FAKE_PODS[index % len(FAKE_PODS)]
        for index, pod in enumerate(pods)
    }
    return owner_map, pod_map


def anonymize_process_line(line: str, row_index: int) -> str:
    parts = line.split(maxsplit=7)
    if len(parts) < 8:
        return line
    gpu, owner, pod, _pid, sm, mem, up, _cmd = parts
    cmd = FAKE_CMDS[row_index % len(FAKE_CMDS)]
    fake_pid = str(12034 + row_index)
    return f"{gpu:>3}  {owner:<8}  {pod:<42}  {fake_pid:>7}  {sm:>4}  {mem:>7}  {up:>7}  {cmd}"


def anonymize(text: str) -> str:
    text = strip_ansi(text)
    owner_map, pod_map = discover_private_tokens(text)
    for real, fake in sorted(pod_map.items(), key=lambda item: -len(item[0])):
        text = re.sub(rf"(?<![A-Za-z0-9_.-]){re.escape(real)}(?![A-Za-z0-9_.-])", fake, text)
    for real, fake in sorted(owner_map.items(), key=lambda item: -len(item[0])):
        text = re.sub(rf"(?<![A-Za-z0-9_.-]){re.escape(real)}(?![A-Za-z0-9_.-])", fake, text)

    rows = []
    proc_index = 0
    for line in text.splitlines():
        if re.match(r"^\s*\d+\s+\S+\s+\S+\s+\d+\s+\S+\s+\S+\s+\S+\s+", line):
            line = anonymize_process_line(line, proc_index)
            proc_index += 1
        rows.append(line)
    return "\n".join(rows)


def trim_lines(text: str, max_cols: int) -> list[str]:
    rendered = anonymize(text) if os.environ.get("SGPU_README_LIVE") == "1" else strip_ansi(text)
    lines = rendered.splitlines()
    return [line if len(line) <= max_cols else line[: max_cols - 1] + ">" for line in lines]


def style_for(line: str) -> str:
    if line.startswith("SGPU"):
        return "title"
    if line.startswith(("===", "NVIDIA", "Kubernetes", "STORAGE", "Awards", "Leaderboard", "KST")):
        return "section"
    if line.startswith(("GPU ", "OWNER", "data:")):
        return "header"
    if "100%" in line or re.search(r"\s9\d%", line):
        return "crit"
    if " 0%" in line:
        return "ok"
    if "atlas" in line:
        return "ownerA"
    if "nova" in line:
        return "ownerB"
    if "orion" in line:
        return "ownerC"
    if line.startswith("*"):
        return "award"
    return "plain"


COLORS = {
    "title": "#67e8f9",
    "section": "#c084fc",
    "header": "#f8fafc",
    "crit": "#fb7185",
    "ok": "#34d399",
    "ownerA": "#93c5fd",
    "ownerB": "#fde68a",
    "ownerC": "#a7f3d0",
    "award": "#facc15",
    "plain": "#d1d5db",
}


def render_terminal_svg(
    *,
    title: str,
    subtitle: str,
    body: str,
    path: Path,
    width: int,
    max_cols: int,
) -> None:
    lines = trim_lines(body, max_cols)
    font_size = 22
    line_h = 32
    pad_x = 44
    top = 108
    height = top + max(1, len(lines)) * line_h + 50
    text_nodes = []
    for idx, line in enumerate(lines):
        y = top + idx * line_h
        color = COLORS[style_for(line)]
        text_nodes.append(
            f'<text x="{pad_x}" y="{y}" fill="{color}">{html.escape(line)}</text>'
        )
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
  <defs>
    <linearGradient id="bg" x1="0" x2="1" y1="0" y2="1">
      <stop offset="0%" stop-color="#06101f"/>
      <stop offset="48%" stop-color="#0b1220"/>
      <stop offset="100%" stop-color="#111827"/>
    </linearGradient>
    <radialGradient id="glow" cx="18%" cy="0%" r="68%">
      <stop offset="0%" stop-color="#0891b2" stop-opacity="0.28"/>
      <stop offset="70%" stop-color="#0891b2" stop-opacity="0"/>
    </radialGradient>
    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
      <feDropShadow dx="0" dy="20" stdDeviation="20" flood-color="#000000" flood-opacity="0.38"/>
    </filter>
  </defs>
  <rect width="100%" height="100%" fill="#020617"/>
  <rect x="24" y="24" width="{width - 48}" height="{height - 48}" rx="24" fill="url(#bg)" stroke="#1f2937" filter="url(#shadow)"/>
  <rect x="24" y="24" width="{width - 48}" height="{height - 48}" rx="24" fill="url(#glow)"/>
  <circle cx="64" cy="60" r="8" fill="#fb7185"/>
  <circle cx="94" cy="60" r="8" fill="#facc15"/>
  <circle cx="124" cy="60" r="8" fill="#34d399"/>
  <text x="156" y="69" fill="#f8fafc" font-size="26" font-weight="750" font-family="Inter, Segoe UI, Arial, sans-serif">{html.escape(title)}</text>
  <text x="{width - 44}" y="69" fill="#94a3b8" font-size="18" text-anchor="end" font-family="Inter, Segoe UI, Arial, sans-serif">{html.escape(subtitle)}</text>
  <g font-family="Cascadia Mono, JetBrains Mono, Consolas, monospace" font-size="{font_size}" xml:space="preserve">
    {''.join(text_nodes)}
  </g>
</svg>
'''
    path.write_text(svg, encoding="utf-8")


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    dashboard = capture(["python", "-m", "sgpu", "once", "--no-color"], SAMPLE_DASHBOARD)
    multi = capture(["python", "-m", "sgpu", "--all", "once", "--no-color"], SAMPLE_MULTI_NODE)
    apps = capture(["python", "-m", "sgpu", "apps", "--no-color"], SAMPLE_APPS)
    stats = capture(["python", "-m", "sgpu", "stats", "30", "--no-color"], SAMPLE_STATS)

    render_terminal_svg(
        title="SGPU live dashboard",
        subtitle="H200 node visibility from kubectl",
        body=dashboard,
        path=OUT / "sgpu-hero.svg",
        width=1600,
        max_cols=118,
    )
    render_terminal_svg(
        title="Multi-node survey",
        subtitle="sgpu --all once across both H200 nodes",
        body=multi,
        path=OUT / "sgpu-multinode.svg",
        width=1600,
        max_cols=118,
    )
    render_terminal_svg(
        title="Process to pod attribution",
        subtitle="owners, pods, PIDs, memory and uptime",
        body=apps,
        path=OUT / "sgpu-processes.svg",
        width=1480,
        max_cols=108,
    )
    render_terminal_svg(
        title="Usage stats and activity",
        subtitle="cluster pulse, momentum, GPU-hours and KST heatmap",
        body=stats,
        path=OUT / "sgpu-stats.svg",
        width=1480,
        max_cols=108,
    )


if __name__ == "__main__":
    main()
