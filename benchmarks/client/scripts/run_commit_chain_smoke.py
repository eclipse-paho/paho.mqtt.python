#!/usr/bin/env python3
"""Run core-smoke suite across SUT SHAs and build commit-delta report.

Usage (from repo root, broker already up):
  python benchmarks/client/scripts/run_commit_chain_smoke.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO = Path(__file__).resolve().parents[3]
CLIENT = REPO / "benchmarks" / "client"
RESULTS = CLIENT / "results"
WT_ROOT = Path("/tmp/paho-sut-chain")

CHAIN: List[Tuple[str, str]] = [
    ("a734a9c", "baseline pre-optimisations (parent of 238eee8)"),
    ("f2aaa76", "perf: cache MQTT v5 property metadata lookups"),
    ("92008c1", "perf: reduce receive message dispatch overhead"),
    ("6f6869c", "perf: reduce publish write-path overhead"),
    ("65e1671", "fix: avoid missed sockpair wakeup during loop_start"),
    ("6bb33c5", "perf: fast-path small remaining length encoding"),
    ("90afdb9", "perf: speed up inbound packet parse path (plan 01)"),
    ("5bbbd95", "perf: micro-optimize MQTTMatcher.iter_match"),
    ("4e5221b", "perf: skip PUBLISH topic decode when logging is disabled"),
    ("9e502e7", "perf: speed up rich MQTT v5 property decoding"),
    ("07e6cd9", "perf: optimize network hot paths"),
    ("7c33f50", "perf: decode buffered packets directly"),
    ("8011c9f", "perf: batch inflight refill after ACK bursts"),
    ("be0d5b2", "perf: stage reconnect replay in bounded batches"),
    ("6634d03", "perf: segment large immutable publish payloads"),
]

TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
SEED = 42
BROKER = "127.0.0.1:11883"


def point_key(scenario: str, point: dict) -> str:
    parts = [
        scenario,
        f"payload={point.get('payload')}",
        f"qos_publish={point.get('qos_publish')}",
        f"qos_subscribe={point.get('qos_subscribe')}",
        f"cadence={point.get('cadence')}",
        f"inflight={point.get('inflight')}",
        f"subscription={point.get('subscription')}",
        f"topic_topology={point.get('topic_topology')}",
        f"callback_filters={point.get('callback_filters')}",
        f"load_fraction={point.get('load_fraction')}",
        f"protocol={point.get('protocol')}",
        f"properties_profile={point.get('properties_profile')}",
    ]
    return " · ".join(str(p) for p in parts if not str(p).endswith("=None"))


def flatten_suite(suite_payload: dict) -> List[dict]:
    rows = []
    for scen in suite_payload.get("scenarios", []):
        name = scen.get("scenario", "?")
        for block in scen.get("results", []):
            point = block.get("point") or {}
            summary = block.get("summary") or {}
            runs = block.get("runs") or []
            statuses = [r.get("status") for r in runs]
            primary = None
            for r in runs:
                if r.get("primary_msgs_per_s") is not None:
                    primary = r.get("primary_msgs_per_s")
                    break
            median = summary.get("median")
            if median is None:
                median = primary
            rows.append(
                {
                    "scenario": name,
                    "key": point_key(name, point),
                    "point": point,
                    "median": median,
                    "status": statuses[0] if len(statuses) == 1 else statuses,
                    "valid": sum(1 for s in statuses if s == "valid"),
                    "runs": len(statuses),
                }
            )
    return rows


def write_summary(sha: str, subject: str, suite_payload: dict, out_stem: Path) -> dict:
    rows = flatten_suite(suite_payload)
    summary = {
        "sha": sha,
        "subject": subject,
        "timestamp": TS,
        "profile": "smoke",
        "suite": "core",
        "seed": SEED,
        "source_root": suite_payload.get("scenarios", [{}])[0].get("source_root")
        if suite_payload.get("scenarios")
        else None,
        "estimate": suite_payload.get("estimate"),
        "points": [
            {
                "key": r["key"],
                "scenario": r["scenario"],
                "median_msgs_per_s": r["median"],
                "valid_runs": r["valid"],
                "runs": r["runs"],
                "status": r["status"],
            }
            for r in rows
        ],
    }
    out_stem.with_suffix(".summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return summary


def run_one(sha: str, subject: str) -> Path:
    source = WT_ROOT / sha
    if not (source / "src" / "paho").is_dir():
        raise SystemExit(f"missing worktree for {sha}: {source}")
    stem = RESULTS / f"core-smoke-{sha}-{TS}"
    out_json = Path(str(stem) + ".json")
    log_path = Path(str(stem) + ".log")
    cmd = [
        sys.executable,
        str(CLIENT / "run.py"),
        "run",
        "--suite",
        "core",
        "--profile",
        "smoke",
        "--source",
        str(source),
        "--broker",
        BROKER,
        "--seed",
        str(SEED),
        "--output",
        str(out_json),
    ]
    print(f"\n=== [{sha}] {subject} ===", flush=True)
    print(" ".join(cmd), flush=True)
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.run(cmd, cwd=str(REPO), stdout=log, stderr=subprocess.STDOUT, text=True)
    elapsed = time.time() - t0
    print(f"exit={proc.returncode} elapsed_s={elapsed:.1f} out={out_json}", flush=True)
    if proc.returncode != 0:
        raise SystemExit(f"run failed for {sha}; see {log_path}")
    payload = json.loads(out_json.read_text(encoding="utf-8"))
    write_summary(sha, subject, payload, stem)
    # Manifest pointer for the chain
    return out_json


def pct_delta(curr: Optional[float], prev: Optional[float]) -> Optional[float]:
    if curr is None or prev is None or prev == 0:
        return None
    return (curr / prev) - 1.0


def fmt_rate(v: Optional[float]) -> str:
    if v is None:
        return "—"
    if abs(v) >= 1000:
        return f"{v:,.0f}"
    return f"{v:,.1f}"


def fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    return f"{v * 100:+.1f}%"


def build_report(summaries: List[dict]) -> str:
    lines: List[str] = []
    lines.append("# Client benchmark deltas — core smoke since plan 1")
    lines.append("")
    lines.append(f"- Timestamp UTC: `{TS}`")
    lines.append(f"- Profile: `smoke` · suite: `core` · seed: `{SEED}`")
    lines.append("- Harness: fixed `benchmarks` branch · SUT via `--source` worktrees")
    lines.append("- Delta% = (commit / previous) - 1 on the point median `msgs/s`")
    lines.append("- Smoke is noisy; treat only large stable gaps as signal")
    lines.append("")
    lines.append("## Commit index")
    lines.append("")
    lines.append("| # | SHA | Subject | Artifact |")
    lines.append("|---:|---|---|---|")
    for i, s in enumerate(summaries):
        art = f"`core-smoke-{s['sha']}-{TS}`"
        lines.append(f"| {i} | `{s['sha']}` | {s['subject']} | {art} |")
    lines.append("")

    step_details = []
    for i in range(1, len(summaries)):
        prev, curr = summaries[i - 1], summaries[i]
        prev_map = {p["key"]: p for p in prev["points"]}
        curr_map = {p["key"]: p for p in curr["points"]}
        keys = sorted(set(prev_map) | set(curr_map))
        deltas = []
        capacity_deltas = []
        detail_rows = []
        for key in keys:
            p = prev_map.get(key)
            c = curr_map.get(key)
            prev_m = None if p is None else p.get("median_msgs_per_s")
            curr_m = None if c is None else c.get("median_msgs_per_s")
            d = pct_delta(curr_m, prev_m)
            if d is not None:
                deltas.append(d)
            status_p = "—" if p is None else p.get("status")
            status_c = "—" if c is None else c.get("status")
            if (
                d is not None
                and "cadence=capacity" in key
                and (key.startswith("pub_") or key.startswith("remaining_length") or key.startswith("duplex_"))
            ):
                capacity_deltas.append(d)
            detail_rows.append((key, prev_m, curr_m, d, status_p, status_c))

        pool = capacity_deltas if capacity_deltas else deltas
        cap_med = None
        if pool:
            pool = sorted(pool)
            cap_med = pool[len(pool) // 2]
        valid_curr = sum(1 for p in curr["points"] if p.get("valid"))
        step_details.append((i, prev, curr, detail_rows, cap_med, valid_curr))

    lines.append("## Step summary (median delta% on capacity publish points)")
    lines.append("")
    lines.append("| Step | Prev → Curr | Median delta% (capacity pub*) | Valid points curr |")
    lines.append("|---|---|---:|---:|")
    for i, prev, curr, detail_rows, cap_med, valid_curr in step_details:
        lines.append(
            f"| {i - 1}→{i} | `{prev['sha']}` → `{curr['sha']}` | {fmt_pct(cap_med)} | "
            f"{valid_curr}/{len(curr['points'])} |"
        )
    lines.append("")
    lines.append(
        "\\* Points whose key contains `cadence=capacity` and a "
        "`pub_*` / `remaining_length*` / `duplex_*` scenario."
    )
    lines.append("")

    for i, prev, curr, detail_rows, cap_med, valid_curr in step_details:
        lines.append(f"## Step {i - 1}→{i}: `{prev['sha']}` → `{curr['sha']}`")
        lines.append("")
        lines.append(f"- **Previous:** `{prev['sha']}` — {prev['subject']}")
        lines.append(f"- **Current:** `{curr['sha']}` — {curr['subject']}")
        lines.append(f"- **Median delta% (capacity pub*):** {fmt_pct(cap_med)}")
        lines.append("")
        lines.append("| Point | prev msg/s | curr msg/s | delta% | prev | curr |")
        lines.append("|---|---:|---:|---:|---|---|")
        for key, prev_m, curr_m, d, status_p, status_c in detail_rows:
            lines.append(
                f"| {key} | {fmt_rate(prev_m)} | {fmt_rate(curr_m)} | {fmt_pct(d)} | {status_p} | {status_c} |"
            )
        lines.append("")

    lines.append("## Caveats")
    lines.append("")
    lines.append("- `smoke` profile: short windows, 1 run/point, marked non-comparable for fine verdicts.")
    lines.append("- Same harness, same local broker, seed 42; host load is not tightly controlled.")
    lines.append("- `inconclusive` / `not_implemented:*` points have no reliable rate (shown as —).")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    RESULTS.mkdir(parents=True, exist_ok=True)
    manifest = []
    summaries = []
    for sha, subject in CHAIN:
        out = run_one(sha, subject)
        stem = RESULTS / f"core-smoke-{sha}-{TS}"
        summary = json.loads(stem.with_suffix(".summary.json").read_text(encoding="utf-8"))
        summaries.append(summary)
        manifest.append({"sha": sha, "subject": subject, "json": str(out.name), "summary": stem.with_suffix(".summary.json").name})

    (RESULTS / f"commit-chain-manifest-{TS}.json").write_text(
        json.dumps({"timestamp": TS, "chain": manifest}, indent=2) + "\n", encoding="utf-8"
    )
    report = build_report(summaries)
    report_path = RESULTS / "commit-delta-since-plan1-smoke.md"
    report_path.write_text(report, encoding="utf-8")
    print(f"\nWrote {report_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
