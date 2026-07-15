#!/usr/bin/env python3
"""Run brokerless Paho MQTT Python benchmarks."""

from __future__ import print_function

import argparse
import os
import sys


HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, "src")
# Prefer this directory for local modules (micro_harness, scenarios, fakes)
# so imports do not collide with benchmarks/client/harness.py when both are
# on sys.path (e.g. under pytest).
if HERE in sys.path:
    sys.path.remove(HERE)
sys.path.insert(0, HERE)
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from micro_harness import build_result, write_json  # noqa: E402
from scenarios import SCENARIOS  # noqa: E402


def _selected_scenarios(name):
    if name == "all":
        return list(SCENARIOS)
    for scenario in SCENARIOS:
        if scenario.name == name:
            return [scenario]
    raise KeyError("unknown scenario: {}".format(name))


def _print_scenarios():
    for scenario in SCENARIOS:
        print("{:<40} {:<18} default_iterations={}".format(
            scenario.name,
            scenario.group,
            scenario.default_iterations,
        ))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default="all", help="Scenario name or 'all'")
    parser.add_argument("--runs", type=int, default=7, help="Measured runs, minimum 5")
    parser.add_argument("--warmups", type=int, default=2, help="Warmup runs")
    parser.add_argument("--iterations", type=int, default=None, help="Override scenario iteration count")
    parser.add_argument("--output", help="Write JSON result to this path")
    parser.add_argument("--no-tracemalloc", action="store_true", help="Disable allocation measurement")
    parser.add_argument("--list", action="store_true", help="List scenarios and exit")
    args = parser.parse_args(argv)

    if args.list:
        _print_scenarios()
        return 0

    if args.runs < 5:
        parser.error("--runs must be at least 5")
    if args.warmups < 0:
        parser.error("--warmups must be >= 0")
    if args.iterations is not None and args.iterations < 1:
        parser.error("--iterations must be >= 1")

    try:
        scenarios = _selected_scenarios(args.scenario)
    except KeyError as exc:
        parser.error(str(exc))

    result = build_result(
        scenarios=scenarios,
        runs=args.runs,
        warmups=args.warmups,
        iterations_override=args.iterations,
        use_tracemalloc=not args.no_tracemalloc,
    )
    write_json(result, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
