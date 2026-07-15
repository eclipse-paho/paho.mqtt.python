#!/usr/bin/env python3
"""Plan 28: fresh-process import benchmark for Paho client and helpers."""

from __future__ import annotations

import argparse
import json
import os
import resource
import statistics
import subprocess
import sys
import time
from pathlib import Path


def _measure_worker(source: str, mode: str) -> dict:
    sys.path.insert(0, str(Path(source).resolve() / "src"))
    if mode.endswith("_no_socks"):
        # Match an installation without the optional PySocks dependency.
        sys.modules["socks"] = None
    modules_before = set(sys.modules)
    urllib_before = "urllib.request" in modules_before
    cpu_start = time.process_time()
    wall_start = time.perf_counter()

    if mode in ("client_no_socks", "client_proxy_lookup"):
        import paho.mqtt.client as imported
        if mode == "client_proxy_lookup":
            from paho.mqtt.enums import CallbackAPIVersion

            client = imported.Client(callback_api_version=CallbackAPIVersion.VERSION2)
            client._host = "localhost"
            client._get_proxy()
    elif mode == "publish_helper_no_socks":
        import paho.mqtt.publish as imported
    else:
        raise ValueError(f"unknown mode: {mode}")

    wall = time.perf_counter() - wall_start
    cpu = time.process_time() - cpu_start
    modules_after = set(sys.modules)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "mode": mode,
        "source": str(Path(source).resolve()),
        "module": str(Path(imported.__file__).resolve()),
        "wall_seconds": wall,
        "cpu_seconds": cpu,
        "new_module_count": len(modules_after - modules_before),
        "urllib_request_before": urllib_before,
        "urllib_request_after": "urllib.request" in modules_after,
        "max_rss_kib": usage.ru_maxrss,
    }


def _run_worker(source: str, mode: str) -> dict:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker-source", source,
        "--worker-mode", mode,
    ]
    process = subprocess.run(command, check=True, text=True, capture_output=True)  # noqa: S603
    return json.loads(process.stdout)


def _summary(rows: list[dict]) -> dict:
    return {
        "wall_ms": statistics.median(row["wall_seconds"] for row in rows) * 1000,
        "cpu_ms": statistics.median(row["cpu_seconds"] for row in rows) * 1000,
        "new_module_count": statistics.median(row["new_module_count"] for row in rows),
        "max_rss_kib": statistics.median(row["max_rss_kib"] for row in rows),
        "urllib_request_loaded": sorted({row["urllib_request_after"] for row in rows}),
        "wall_range_ms": [
            min(row["wall_seconds"] for row in rows) * 1000,
            max(row["wall_seconds"] for row in rows) * 1000,
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-source")
    parser.add_argument("--candidate-source")
    parser.add_argument("--runs", type=int, default=15)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--output")
    parser.add_argument("--worker-source", help=argparse.SUPPRESS)
    parser.add_argument(
        "--worker-mode",
        choices=("client_no_socks", "publish_helper_no_socks", "client_proxy_lookup"),
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args()

    if args.worker_source:
        if not args.worker_mode:
            parser.error("--worker-mode is required with --worker-source")
        print(json.dumps(_measure_worker(args.worker_source, args.worker_mode), sort_keys=True))
        return 0
    if not args.baseline_source or not args.candidate_source:
        parser.error("--baseline-source and --candidate-source are required")
    if args.runs < 5 or args.warmups < 0:
        parser.error("use at least 5 runs and a non-negative warmup count")

    sources = {
        "baseline": args.baseline_source,
        "candidate": args.candidate_source,
    }
    modes = ("client_no_socks", "publish_helper_no_socks", "client_proxy_lookup")
    rows = {
        mode: {"baseline": [], "candidate": []}
        for mode in modes
    }

    for mode in modes:
        for source in sources.values():
            for _ in range(args.warmups):
                _run_worker(source, mode)
        for _ in range(args.runs):
            for variant in ("baseline", "candidate", "candidate", "baseline"):
                rows[mode][variant].append(_run_worker(sources[variant], mode))

    summaries = {
        mode: {variant: _summary(samples) for variant, samples in variants.items()}
        for mode, variants in rows.items()
    }
    deltas = {}
    for mode, variants in summaries.items():
        baseline = variants["baseline"]
        candidate = variants["candidate"]
        deltas[mode] = {
            key: (candidate[key] / baseline[key] - 1.0) * 100.0
            for key in ("wall_ms", "cpu_ms", "new_module_count", "max_rss_kib")
        }

    result = {
        "python": sys.version,
        "cpu_affinity": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "runs_per_version": args.runs * 2,
        "warmups_per_version": args.warmups,
        "order": ["baseline", "candidate", "candidate", "baseline"],
        "summaries": summaries,
        "deltas_percent": deltas,
        "rows": rows,
    }
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
