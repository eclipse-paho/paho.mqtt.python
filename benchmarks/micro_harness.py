"""Shared benchmark harness code."""

from __future__ import absolute_import

import gc
import json
import os
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional


SCHEMA_VERSION = 1


@dataclass
class Scenario:
    name: str
    group: str
    unit: str
    default_iterations: int
    func: Callable[[int], None]
    operations_per_iteration: int = 1


def percentile(values, percent):
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (percent / 100.0)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _rusage():
    try:
        import resource
    except ImportError:
        return None
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime, usage.ru_stime


def _git_output(args):
    try:
        proc = subprocess.Popen(
            ["git"] + args,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, _ = proc.communicate(timeout=2)
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    return stdout.decode("utf-8", "replace").strip()


def environment_metadata():
    dirty = None
    status = _git_output(["status", "--short"])
    if status is not None:
        dirty = bool(status)

    return {
        "schema_version": SCHEMA_VERSION,
        "timestamp_utc": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "python_version": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "executable": sys.executable,
        "git_commit": _git_output(["rev-parse", "HEAD"]),
        "git_dirty": dirty,
    }


def _allocation_stats(snapshot):
    stats = snapshot.statistics("filename")
    return sum(stat.count for stat in stats), sum(stat.size for stat in stats)


def _run_once(scenario, iterations, use_tracemalloc):
    gc.collect()
    was_enabled = gc.isenabled()
    start_usage = _rusage()

    if use_tracemalloc:
        tracemalloc.start()
        start_snapshot = tracemalloc.take_snapshot()
    else:
        start_snapshot = None

    gc.disable()
    start = time.perf_counter()
    try:
        scenario.func(iterations)
    finally:
        end = time.perf_counter()
        if was_enabled:
            gc.enable()

    end_usage = _rusage()

    allocation_count = None
    allocation_bytes = None
    allocation_peak = None
    if use_tracemalloc and start_snapshot is not None:
        current, allocation_peak = tracemalloc.get_traced_memory()
        end_snapshot = tracemalloc.take_snapshot()
        count_end, size_end = _allocation_stats(end_snapshot)
        count_start, size_start = _allocation_stats(start_snapshot)
        allocation_count = count_end - count_start
        allocation_bytes = size_end - size_start
        # Keep current referenced so older linters do not consider it unused.
        allocation_peak = max(allocation_peak, current)
        tracemalloc.stop()

    user_cpu = None
    system_cpu = None
    if start_usage is not None and end_usage is not None:
        user_cpu = end_usage[0] - start_usage[0]
        system_cpu = end_usage[1] - start_usage[1]

    return {
        "wall_seconds": end - start,
        "user_cpu_seconds": user_cpu,
        "system_cpu_seconds": system_cpu,
        "allocation_count": allocation_count,
        "allocation_bytes": allocation_bytes,
        "allocation_peak_bytes": allocation_peak,
    }


def run_scenario(scenario, runs, warmups, iterations, use_tracemalloc):
    for _ in range(warmups):
        scenario.func(iterations)

    run_results = []
    for _ in range(runs):
        run_results.append(_run_once(scenario, iterations, use_tracemalloc))

    operations = iterations * scenario.operations_per_iteration
    wall_times = [item["wall_seconds"] for item in run_results]
    per_operation = [item["wall_seconds"] / operations for item in run_results]
    median_wall = statistics.median(wall_times)
    operations_per_second = operations / median_wall if median_wall else 0.0

    allocation_counts = [
        item["allocation_count"] for item in run_results
        if item["allocation_count"] is not None
    ]
    allocation_bytes = [
        item["allocation_bytes"] for item in run_results
        if item["allocation_bytes"] is not None
    ]
    allocation_peaks = [
        item["allocation_peak_bytes"] for item in run_results
        if item["allocation_peak_bytes"] is not None
    ]

    return {
        "name": scenario.name,
        "group": scenario.group,
        "iterations": iterations,
        "operations": operations,
        "operation_unit": scenario.unit,
        "runs": runs,
        "warmups": warmups,
        "median_wall_seconds": median_wall,
        "min_wall_seconds": min(wall_times),
        "max_wall_seconds": max(wall_times),
        "p50_seconds_per_operation": percentile(per_operation, 50.0),
        "p95_seconds_per_operation": percentile(per_operation, 95.0),
        "operations_per_second": operations_per_second,
        "allocation_count": int(statistics.median(allocation_counts)) if allocation_counts else None,
        "allocation_bytes": int(statistics.median(allocation_bytes)) if allocation_bytes else None,
        "allocation_peak_bytes": int(statistics.median(allocation_peaks)) if allocation_peaks else None,
        "run_results": run_results,
    }


def build_result(scenarios, runs, warmups, iterations_override, use_tracemalloc):
    results = []
    for scenario in scenarios:
        iterations = iterations_override or scenario.default_iterations
        results.append(run_scenario(scenario, runs, warmups, iterations, use_tracemalloc))
    return {
        "metadata": environment_metadata(),
        "scenarios": results,
    }


def write_json(result, output):
    text = json.dumps(result, indent=2, sort_keys=True)
    if output:
        directory = os.path.dirname(os.path.abspath(output))
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        with open(output, "w", encoding="utf-8") as f:
            f.write(text)
            f.write("\n")
    else:
        print(text)
