#!/usr/bin/env python3
"""Compare two benchmark result files."""

from __future__ import print_function

import argparse
import json
import sys


GAIN_THRESHOLD = 5.0
REGRESSION_THRESHOLD = -3.0


def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _scenario_map(result):
    return {item["name"]: item for item in result.get("scenarios", [])}


def _delta_percent(baseline, candidate):
    if baseline == 0:
        return None
    return ((candidate - baseline) / baseline) * 100.0


def _allocation_delta(baseline, candidate):
    base_alloc = baseline.get("allocation_bytes")
    cand_alloc = candidate.get("allocation_bytes")
    if base_alloc is None or cand_alloc is None:
        return "n/a"
    delta = cand_alloc - base_alloc
    sign = "+" if delta > 0 else ""
    return "{}{}".format(sign, delta)


def _verdict(delta):
    if delta is None:
        return "noise"
    if delta >= GAIN_THRESHOLD:
        return "gain"
    if delta <= REGRESSION_THRESHOLD:
        return "regression"
    return "noise"


def compare(baseline, candidate):
    baseline_scenarios = _scenario_map(baseline)
    candidate_scenarios = _scenario_map(candidate)
    names = sorted(set(baseline_scenarios) | set(candidate_scenarios))

    rows = []
    regressions = 0
    matches = 0

    for name in names:
        base = baseline_scenarios.get(name)
        cand = candidate_scenarios.get(name)
        if base is None or cand is None:
            rows.append((name, "n/a", "n/a", "n/a", "n/a", "missing"))
            continue

        matches += 1
        base_ops = float(base["operations_per_second"])
        cand_ops = float(cand["operations_per_second"])
        delta = _delta_percent(base_ops, cand_ops)
        verdict = _verdict(delta)
        if verdict == "regression":
            regressions += 1

        delta_text = "n/a" if delta is None else "{:+.2f}%".format(delta)
        rows.append((
            name,
            "{:.2f}".format(base_ops),
            "{:.2f}".format(cand_ops),
            delta_text,
            _allocation_delta(base, cand),
            verdict,
        ))

    if matches == 0:
        return rows, 2
    if regressions:
        return rows, 1
    return rows, 0


def print_rows(rows):
    headers = ("scenario", "baseline ops/s", "candidate ops/s", "delta", "alloc bytes", "verdict")
    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    def fmt(row):
        return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))

    print(fmt(headers))
    print(fmt(tuple("-" * width for width in widths)))
    for row in rows:
        print(fmt(row))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", help="Baseline JSON result file")
    parser.add_argument("candidate", help="Candidate JSON result file")
    args = parser.parse_args(argv)

    try:
        baseline = _load(args.baseline)
        candidate = _load(args.candidate)
        rows, exit_code = compare(baseline, candidate)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2

    print_rows(rows)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
