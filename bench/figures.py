#!/usr/bin/env python3
"""Draws the README figures from the benchmark results, as SVG (no dependencies beyond Python 3).

    python3 bench/figures.py            # reads bench/results/*.json, writes docs/figures/*.svg

The figures follow the light/dark setting of whoever views them. Every bar carries its value as text, and
the same numbers are in tables in README.md and docs/phase-5.md, so nothing depends on telling colours apart.
"""

import glob
import json
import os
import statistics
from html import escape

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "docs", "figures")

ENGINES = [("stratakv", "StrataKV"), ("rocksdb", "RocksDB"), ("sqlite", "SQLite")]
WORKLOADS = [
    ("fillrandom", "Random writes"),
    ("overwrite", "Random overwrites"),
    ("fillseq (batches of 1000)", "Bulk load (batches of 1,000)"),
    ("fillsync (fsync each)", "Durable writes (fsync each)"),
    ("readrandom", "Random reads"),
    ("readmissing", "Reads of missing keys"),
    ("seekrandom (100 keys)", "Scans of 100 keys"),
    ("readseq (full scan)", "Full scan"),
]

# Colours by role; each engine keeps its colour in every figure. Validated as a set (light and dark) with the
# data-viz palette checker: CVD-safe for all pairs, the light aqua relies on the value labels for contrast.
STYLE = """
  svg { --surface: #fcfcfb; --ink: #0b0b0b; --ink2: #52514e; --muted: #898781; --grid: #e1e0d9; --axis: #c3c2b7;
        --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a; --before: #c3c2b7; }
  @media (prefers-color-scheme: dark) {
    svg { --surface: #1a1a19; --ink: #f0efec; --ink2: #c3c2b7; --muted: #898781; --grid: #2c2c2a; --axis: #383835;
          --s1: #3987e5; --s2: #d95926; --s3: #199e70; --before: #52514e; }
  }
  text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; fill: var(--ink2); font-size: 12px; }
  .title { fill: var(--ink); font-size: 16px; font-weight: 600; }
  .sub { fill: var(--ink2); font-size: 12px; }
  .row { fill: var(--ink); font-size: 12px; }
  .tick { fill: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }
  .val { fill: var(--ink2); font-size: 11px; font-variant-numeric: tabular-nums; }
  .note { fill: var(--ink); font-size: 12px; font-weight: 600; }
  .bg { fill: var(--surface); }
  .grid { stroke: var(--grid); stroke-width: 1; }
  .axis { stroke: var(--axis); stroke-width: 1; }
  .s1 { fill: var(--s1); } .s2 { fill: var(--s2); } .s3 { fill: var(--s3); } .before { fill: var(--before); }
"""


def median_results():
    runs = {}
    for path in sorted(glob.glob(os.path.join(ROOT, "bench", "results", "*.json"))):
        with open(path) as f:
            run = json.load(f)
        runs.setdefault(run["engine"], []).append(run)
    out = {}
    for engine, rs in runs.items():
        out[engine] = {}
        for w, _ in WORKLOADS:
            vals = [r["ops_per_sec"] for run in rs for r in run["results"] if r["workload"] == w]
            out[engine][w] = statistics.median(vals)
    return out, runs


def human(v):
    if v >= 1e6:
        return f"{v / 1e6:.2f} M"
    if v >= 1e3:
        return f"{v / 1e3:.0f} K"
    return f"{v:.0f}"


def bar(x0, y, length, h, cls):
    """A horizontal bar growing right from x0: square at the baseline, 4px rounded at the data end."""
    if length <= 0:
        return ""
    r = min(4, length, h / 2)
    x1 = x0 + length
    return (f'<path class="{cls}" d="M{x0:.1f},{y:.1f} H{x1 - r:.1f} Q{x1:.1f},{y:.1f} {x1:.1f},{y + r:.1f} '
            f'V{y + h - r:.1f} Q{x1:.1f},{y + h:.1f} {x1 - r:.1f},{y + h:.1f} H{x0:.1f} Z"/>')


def legend(x, y, items):
    parts, cx = [], x
    for cls, label in items:
        parts.append(f'<rect class="{cls}" x="{cx}" y="{y - 9}" width="10" height="10" rx="2"/>')
        parts.append(f'<text x="{cx + 16}" y="{y}">{escape(label)}</text>')
        cx += 16 + 7 * len(label) + 22
    return "\n".join(parts)


def svg(width, height, title, desc, body):
    return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
            f'role="img" aria-labelledby="t d">\n<title id="t">{escape(title)}</title>\n<desc id="d">{escape(desc)}</desc>\n'
            f'<style>{STYLE}</style>\n<rect class="bg" width="{width}" height="{height}" rx="8"/>\n{body}\n</svg>\n')


def throughput(med, runs):
    """Grouped bars: each engine's throughput relative to RocksDB's, one group per workload, absolute value labelled."""
    width, left, right = 820, 210, 70
    bar_h, gap, group_gap = 11, 2, 18
    top = 96
    group_h = 3 * bar_h + 2 * gap
    height = top + len(WORKLOADS) * (group_h + group_gap) + 40
    plot_w = width - left - right
    vmax = 2.5
    sx = lambda v: left + plot_w * min(v, vmax) / vmax  # noqa: E731
    body = [
        '<text class="title" x="24" y="34">Throughput relative to RocksDB</text>',
        f'<text class="sub" x="24" y="54">1,000,000 keys, one client thread, median of {len(runs["stratakv"])} runs. '
        'Labels are operations per second; longer is faster.</text>',
        legend(24, 80, [("s1", "StrataKV"), ("s2", "RocksDB"), ("s3", "SQLite")]),
    ]
    axis_bottom = height - 34
    for t in [0, 0.5, 1, 1.5, 2, 2.5]:
        x = sx(t)
        body.append(f'<line class="{"axis" if t in (0, 1) else "grid"}" x1="{x:.1f}" y1="{top - 6}" x2="{x:.1f}" y2="{axis_bottom}"/>')
        label = "same as RocksDB" if t == 1 else f"{t:g}×"
        body.append(f'<text class="tick" x="{x:.1f}" y="{axis_bottom + 16}" text-anchor="middle">{label}</text>')
    y = top
    for w, label in WORKLOADS:
        base = med["rocksdb"][w]
        body.append(f'<text class="row" x="{left - 12}" y="{y + group_h / 2 + 4:.1f}" text-anchor="end">{escape(label)}</text>')
        for i, (engine, _) in enumerate(ENGINES):
            v = med[engine][w]
            ratio = v / base
            by = y + i * (bar_h + gap)
            body.append(bar(sx(0), by, sx(ratio) - sx(0), bar_h, f"s{i + 1}"))
            clipped = "+" if ratio > vmax else ""
            body.append(f'<text class="val" x="{sx(ratio) + 6:.1f}" y="{by + bar_h - 2:.1f}">{human(v)}{clipped}</text>')
        y += group_h + group_gap
    desc = "; ".join(f"{label}: " + ", ".join(f"{name} {human(med[e][w])} per second" for e, name in ENGINES) for w, label in WORKLOADS)
    return svg(width, height, "Throughput relative to RocksDB", desc, "\n".join(body))


# Measured while fixing what the benchmark found (docs/phase-5.md, "What the benchmark found").
FIXES = [
    ("Scans of 100 keys per second", "higher is better", 410, 26_037, lambda v: f"{v:,.0f}", "63× faster"),
    ("Slowest 1% of bulk-load batches, ms", "lower is better", 26.2, 6.0, lambda v: f"{v:.0f} ms", "4× lower"),
    ("Reopens wrongly refused, of 20,000", "lower is better", 10_571, 0, lambda v: f"{v:,.0f}", "none refused"),
]


def fixes():
    """One small panel per fix (each with its own scale: the units differ), before and after."""
    width, left, right = 820, 24, 24
    panel_w = (width - left - right - 2 * 24) / 3
    top, bar_h = 104, 22
    height = top + 2 * bar_h + 16 + 70
    body = [
        '<text class="title" x="24" y="34">What the benchmark found, and the fixes</text>',
        '<text class="sub" x="24" y="54">Measured before and after each change (100,000 keys for the first two).</text>',
        legend(24, 80, [("before", "Before"), ("s1", "After")]),
    ]
    for n, (label, better, before, after, fmt, note) in enumerate(FIXES):
        x0 = left + n * (panel_w + 24)
        plot_w = panel_w - 70
        vmax = max(before, after)
        body.append(f'<text class="row" x="{x0:.1f}" y="{top - 6}">{escape(label)}</text>')
        for i, (cls, v) in enumerate([("before", before), ("s1", after)]):
            by = top + 6 + i * (bar_h + 4)
            length = plot_w * v / vmax
            body.append(f'<line class="axis" x1="{x0:.1f}" y1="{by - 2}" x2="{x0:.1f}" y2="{by + bar_h + 2}"/>')
            body.append(bar(x0, by, length, bar_h, cls))
            body.append(f'<text class="val" x="{x0 + length + 6:.1f}" y="{by + bar_h / 2 + 4:.1f}">{fmt(v)}</text>')
        ny = top + 6 + 2 * (bar_h + 4) + 22
        body.append(f'<text class="note" x="{x0:.1f}" y="{ny}">{escape(note)}</text>')
        body.append(f'<text class="tick" x="{x0:.1f}" y="{ny + 18}">{escape(better)}</text>')
    desc = "; ".join(f"{label}: before {fmt(b)}, after {fmt(a)} ({note})" for label, _, b, a, fmt, note in FIXES)
    return svg(width, height, "What the benchmark found, and the fixes", desc, "\n".join(body))


def main():
    os.makedirs(OUT, exist_ok=True)
    med, runs = median_results()
    for name, content in [("throughput.svg", throughput(med, runs)), ("fixes.svg", fixes())]:
        with open(os.path.join(OUT, name), "w") as f:
            f.write(content)
        print("wrote", os.path.join("docs", "figures", name))


if __name__ == "__main__":
    main()
