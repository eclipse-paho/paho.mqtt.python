#!/usr/bin/env python3
"""Regenerate commit-delta report from existing summary JSON files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional, Tuple

RESULTS = Path(__file__).resolve().parents[1] / "results"
TS = "20260712T103742Z"
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


def status_of(point: Optional[dict]) -> str:
    if point is None:
        return "missing"
    status = point.get("status")
    if isinstance(status, str):
        return status
    if isinstance(status, list) and status:
        return str(status[0])
    return "valid" if (point.get("valid_runs") or 0) > 0 else "inconclusive"


def is_capacity_pub(key: str) -> bool:
    return "cadence=capacity" in key and (
        key.startswith("pub_") or key.startswith("remaining_length") or key.startswith("duplex_")
    )


def main() -> None:
    summaries = []
    for sha, subject in CHAIN:
        path = RESULTS / f"core-smoke-{sha}-{TS}.summary.json"
        if not path.exists():
            raise SystemExit(f"missing summary: {path}")
        data = json.loads(path.read_text(encoding="utf-8"))
        data["subject"] = subject
        summaries.append(data)
        valid = sum(1 for p in data["points"] if (p.get("valid_runs") or 0) > 0)
        print(f"loaded {sha}: {len(data['points'])} points, valid={valid}")

    lines = [
        "# Différentiels benchmark client — smoke core depuis plan 1",
        "",
        f"- Timestamp UTC: `{TS}`",
        "- Profil: `smoke` · suite: `core` · seed: `42`",
        "- Harness: branche `benchmarks` (fixe) · SUT via `--source` worktrees",
        "- Δ% = (commit / précédent) − 1 sur médiane `msgs/s` du point",
        "- Smoke = bruit élevé ; signes seulement si écarts gros et stables",
        "",
        "## Index des commits",
        "",
        "| # | SHA | Sujet | Artefact |",
        "|---:|---|---|---|",
    ]
    for i, (sha, subject) in enumerate(CHAIN):
        lines.append(f"| {i} | `{sha}` | {subject} | `core-smoke-{sha}-{TS}` |")
    lines.append("")

    steps = []
    for i in range(1, len(CHAIN)):
        prev_sha, prev_subj = CHAIN[i - 1]
        curr_sha, curr_subj = CHAIN[i]
        prev_map = {p["key"]: p for p in summaries[i - 1]["points"]}
        curr_map = {p["key"]: p for p in summaries[i]["points"]}
        keys = sorted(set(prev_map) | set(curr_map))
        capacity_deltas = []
        all_deltas = []
        detail_rows = []
        for key in keys:
            p = prev_map.get(key)
            c = curr_map.get(key)
            prev_m = None if p is None else p.get("median_msgs_per_s")
            curr_m = None if c is None else c.get("median_msgs_per_s")
            d = pct_delta(curr_m, prev_m)
            if d is not None:
                all_deltas.append(d)
                if is_capacity_pub(key):
                    capacity_deltas.append(d)
            detail_rows.append((key, prev_m, curr_m, d, status_of(p), status_of(c)))
        pool = capacity_deltas if capacity_deltas else all_deltas
        cap_med = sorted(pool)[len(pool) // 2] if pool else None
        valid_curr = sum(1 for p in summaries[i]["points"] if (p.get("valid_runs") or 0) > 0)
        steps.append(
            (
                i,
                prev_sha,
                prev_subj,
                curr_sha,
                curr_subj,
                detail_rows,
                cap_med,
                valid_curr,
                len(summaries[i]["points"]),
            )
        )

    lines.extend(
        [
            "## Résumé des pas (Δ% médian sur points capacity publish)",
            "",
            "| Pas | Prev → Curr | Δ% médian (capacity pub*) | Points valid curr |",
            "|---|---|---:|---:|",
        ]
    )
    for i, prev_sha, prev_subj, curr_sha, curr_subj, detail_rows, cap_med, valid_curr, npoints in steps:
        lines.append(
            f"| {i - 1}→{i} | `{prev_sha}` → `{curr_sha}` | {fmt_pct(cap_med)} | {valid_curr}/{npoints} |"
        )
    lines.extend(
        [
            "",
            "\\* Points dont la clé contient `cadence=capacity` et un scénario "
            "`pub_*` / `remaining_length*` / `duplex_*`.",
            "",
        ]
    )

    for i, prev_sha, prev_subj, curr_sha, curr_subj, detail_rows, cap_med, valid_curr, npoints in steps:
        lines.extend(
            [
                f"## Pas {i - 1}→{i}: `{prev_sha}` → `{curr_sha}`",
                "",
                f"- **Précédent:** `{prev_sha}` — {prev_subj}",
                f"- **Courant:** `{curr_sha}` — {curr_subj}",
                f"- **Δ% médian (capacity pub*):** {fmt_pct(cap_med)}",
                "",
                "| Point | prev msg/s | curr msg/s | Δ% | prev | curr |",
                "|---|---:|---:|---:|---|---|",
            ]
        )
        for key, prev_m, curr_m, d, status_p, status_c in detail_rows:
            lines.append(
                f"| {key} | {fmt_rate(prev_m)} | {fmt_rate(curr_m)} | {fmt_pct(d)} | {status_p} | {status_c} |"
            )
        lines.append("")

    lines.extend(
        [
            "## Caveats",
            "",
            "- Profil `smoke`: fenêtres courtes, 1 run/point, marqué non comparable pour verdicts fins.",
            "- Même harness, même broker local, seed 42 ; charge machine non contrôlée finement.",
            "- Les points `inconclusive` / `not_implemented:*` n'ont pas de taux fiable (affichés —).",
            "",
        ]
    )

    report_path = RESULTS / "commit-delta-since-plan1-smoke.md"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {report_path}")
    print()
    print("| Pas | Prev → Curr | Δ% médian | valid |")
    for i, prev_sha, prev_subj, curr_sha, curr_subj, detail_rows, cap_med, valid_curr, npoints in steps:
        print(f"| {i - 1}→{i} | `{prev_sha}` → `{curr_sha}` | {fmt_pct(cap_med)} | {valid_curr}/{npoints} |")


if __name__ == "__main__":
    main()
