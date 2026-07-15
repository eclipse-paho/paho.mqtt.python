#!/usr/bin/env python3
"""Plan 27: compare native and compatibility socket-pair lifecycle costs."""

from __future__ import annotations

import argparse
import json
import os
import select
import statistics
import subprocess
import sys
import time
from pathlib import Path


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    rank = (len(ordered) - 1) * percentile
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _fd_count() -> int | None:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return None


def _measure_worker(args: argparse.Namespace) -> dict:
    source = Path(args.worker_source).resolve()
    sys.path.insert(0, str(source / "src"))

    import paho.mqtt.client as mqtt
    from paho.mqtt.enums import CallbackAPIVersion

    def close_pair(pair) -> None:
        pair[0].close()
        pair[1].close()

    def pair_sample() -> dict:
        cpu_start = time.process_time()
        wall_start = time.perf_counter()
        for _ in range(args.pairs):
            close_pair(mqtt._socketpair_compat())
        wall = time.perf_counter() - wall_start
        cpu = time.process_time() - cpu_start
        return {"wall_per_pair": wall / args.pairs, "cpu_per_pair": cpu / args.pairs}

    def clients_sample() -> dict:
        cpu_start = time.process_time()
        wall_start = time.perf_counter()
        for _ in range(args.clients):
            client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
            client._sockpairR, client._sockpairW = mqtt._socketpair_compat()
            client._reset_sockets(sockpair_only=True)
        wall = time.perf_counter() - wall_start
        cpu = time.process_time() - cpu_start
        return {"wall_per_client": wall / args.clients, "cpu_per_client": cpu / args.clients}

    def wakeup_sample() -> dict:
        read_sock, write_sock = mqtt._socketpair_compat()
        latencies = []
        cpu_start = time.process_time()
        try:
            for _ in range(args.wakeups):
                start = time.perf_counter()
                write_sock.send(b"0")
                readable, _, _ = select.select([read_sock], [], [], 1.0)
                if readable != [read_sock] or read_sock.recv(1) != b"0":
                    raise RuntimeError("socket-pair wakeup was lost or corrupted")
                latencies.append(time.perf_counter() - start)
        finally:
            close_pair((read_sock, write_sock))
        return {
            "median_wakeup": statistics.median(latencies),
            "p95_wakeup": _percentile(latencies, 0.95),
            "cpu_per_wakeup": (time.process_time() - cpu_start) / args.wakeups,
        }

    for _ in range(args.warmups):
        pair_sample()
        clients_sample()
        wakeup_sample()

    fd_before = _fd_count()
    pair_rows = [pair_sample() for _ in range(args.runs)]
    client_rows = [clients_sample() for _ in range(args.runs)]
    wakeup_rows = [wakeup_sample() for _ in range(args.runs)]
    fd_after = _fd_count()

    probe = mqtt._socketpair_compat()
    try:
        socket_metadata = {
            "family": [int(probe[0].family), int(probe[1].family)],
            "type": [int(probe[0].type), int(probe[1].type)],
            "blocking": [probe[0].getblocking(), probe[1].getblocking()],
        }
    finally:
        close_pair(probe)

    return {
        "source": str(source),
        "module": str(Path(mqtt.__file__).resolve()),
        "python": sys.version,
        "runs": args.runs,
        "warmups": args.warmups,
        "pairs_per_run": args.pairs,
        "clients_per_run": args.clients,
        "wakeups_per_run": args.wakeups,
        "fd_before": fd_before,
        "fd_after": fd_after,
        "socket_metadata": socket_metadata,
        "pair_rows": pair_rows,
        "client_rows": client_rows,
        "wakeup_rows": wakeup_rows,
    }


def _summarize(samples: list[dict]) -> dict:
    categories = {
        "pair_wall_us": ("pair_rows", "wall_per_pair"),
        "pair_cpu_us": ("pair_rows", "cpu_per_pair"),
        "client_wall_us": ("client_rows", "wall_per_client"),
        "client_cpu_us": ("client_rows", "cpu_per_client"),
        "wakeup_median_us": ("wakeup_rows", "median_wakeup"),
        "wakeup_p95_us": ("wakeup_rows", "p95_wakeup"),
        "wakeup_cpu_us": ("wakeup_rows", "cpu_per_wakeup"),
    }
    summary = {}
    for output_key, (rows_key, value_key) in categories.items():
        values = [row[value_key] for sample in samples for row in sample[rows_key]]
        summary[output_key] = statistics.median(values) * 1e6
    summary["fd_deltas"] = [
        None if sample["fd_before"] is None else sample["fd_after"] - sample["fd_before"]
        for sample in samples
    ]
    summary["socket_metadata"] = samples[0]["socket_metadata"]
    return summary


def _run_worker(source: str, args: argparse.Namespace) -> dict:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker-source", source,
        "--runs", str(args.runs),
        "--warmups", str(args.warmups),
        "--pairs", str(args.pairs),
        "--clients", str(args.clients),
        "--wakeups", str(args.wakeups),
    ]
    process = subprocess.run(command, check=True, text=True, capture_output=True)  # noqa: S603
    return json.loads(process.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-source")
    parser.add_argument("--candidate-source")
    parser.add_argument("--worker-source", help=argparse.SUPPRESS)
    parser.add_argument("--runs", type=int, default=15)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--pairs", type=int, default=1000)
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--wakeups", type=int, default=1000)
    parser.add_argument("--output")
    args = parser.parse_args()

    if args.worker_source:
        print(json.dumps(_measure_worker(args), sort_keys=True))
        return 0
    if not args.baseline_source or not args.candidate_source:
        parser.error("--baseline-source and --candidate-source are required")
    if args.runs < 5 or args.warmups < 0:
        parser.error("use at least 5 runs and a non-negative warmup count")

    samples = {"baseline": [], "candidate": []}
    order = ("baseline", "candidate", "candidate", "baseline")
    sources = {
        "baseline": args.baseline_source,
        "candidate": args.candidate_source,
    }
    for variant in order:
        samples[variant].append(_run_worker(sources[variant], args))

    summaries = {variant: _summarize(rows) for variant, rows in samples.items()}
    deltas = {}
    for key, baseline in summaries["baseline"].items():
        if not key.endswith("_us"):
            continue
        candidate = summaries["candidate"][key]
        deltas[key] = (candidate / baseline - 1.0) * 100.0

    result = {
        "order": order,
        "summaries": summaries,
        "deltas_percent": deltas,
        "samples": samples,
    }
    text = json.dumps(result, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
