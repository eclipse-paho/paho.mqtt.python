#!/usr/bin/env python3
"""Paho MQTT client end-to-end benchmark CLI."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CLIENT_DIR = Path(__file__).resolve().parent
_client_dir = str(CLIENT_DIR)
while _client_dir in sys.path:
    sys.path.remove(_client_dir)
sys.path.insert(0, _client_dir)

from broker import broker_down, broker_up, ensure_certs  # noqa: E402
from harness import calibrate, compare_sources, run_scenario, run_suite  # noqa: E402
from network import PROFILES  # noqa: E402
from scenarios import SCENARIO_BY_NAME, estimate_suite, list_scenarios  # noqa: E402
from control import write_json  # noqa: E402

REPO_ROOT = CLIENT_DIR.parent.parent


def cmd_broker(args: argparse.Namespace) -> int:
    if args.action == "up":
        ensure_certs()
        meta = broker_up(wait=True)
        print(json.dumps(meta, indent=2))
        return 0
    if args.action == "down":
        broker_down()
        print("broker down")
        return 0
    raise SystemExit(f"unknown broker action: {args.action}")


def cmd_list(args: argparse.Namespace) -> int:
    scenarios = list_scenarios(args.suite)
    for scenario in scenarios:
        tags = ",".join(scenario.tags)
        print(f"{scenario.name:<28} suite={scenario.suite:<4} tags={tags:<28} {scenario.description}")
    if args.suite:
        est = estimate_suite(args.suite, args.profile, 7 if args.profile == "standard" else 1)
        print(
            f"\nEstimate ({args.profile}): {est['points']} points, "
            f"{est['runs_per_point']} runs/point, ~{est['estimated_minutes']} min"
        )
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    source = str(Path(args.source or REPO_ROOT).resolve())
    if args.suite:
        result = run_suite(
            args.suite,
            source_root=source,
            profile=args.profile,
            runs=args.runs,
            broker=args.broker,
            network=args.network,
            output=None,
            load_profile_path=args.load_profile,
            seed=args.seed,
        )
        if args.output:
            write_json(args.output, result)
        else:
            print(json.dumps({"suite": result["suite"], "estimate": result["estimate"]}, indent=2))
        return 0

    if not args.scenario:
        print("error: provide --scenario or --suite", file=sys.stderr)
        return 2
    if args.scenario not in SCENARIO_BY_NAME:
        print(f"error: unknown scenario {args.scenario}", file=sys.stderr)
        return 2

    result = run_scenario(
        args.scenario,
        source_root=source,
        profile=args.profile,
        runs=args.runs,
        broker=args.broker,
        network=args.network,
        output=args.output,
        load_profile_path=args.load_profile,
        seed=args.seed,
    )
    if not args.output:
        # Compact stdout summary.
        for block in result.get("results", []):
            point = block["point"]
            summary = block["summary"]
            print(
                f"{result['scenario']} payload={point.get('payload')} qos={point.get('qos_publish')} "
                f"median_msgs_per_s={summary.get('median')} status_runs="
                f"{sum(1 for r in block['runs'] if r.get('status') == 'valid')}/{len(block['runs'])}"
            )
    return 0


def cmd_calibrate(args: argparse.Namespace) -> int:
    source = str(Path(args.source or REPO_ROOT).resolve())
    payload = calibrate(source, args.output, profile=args.profile)
    print(json.dumps({"capacity_msgs_per_s": payload.get("capacity_msgs_per_s"), "fractions": payload.get("fractions")}, indent=2))
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    payload = compare_sources(
        args.baseline_source,
        args.candidate_source,
        args.scenario,
        blocks=args.blocks,
        point_index=args.point_index,
        profile=args.profile,
        output=args.output,
        load_profile_path=args.load_profile,
    )
    print(json.dumps({"verdict": payload.get("verdict"), "order": payload.get("order")}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command")

    broker_p = sub.add_parser("broker", help="Manage local Mosquitto via docker compose")
    broker_p.add_argument("action", choices=["up", "down"])
    broker_p.set_defaults(func=cmd_broker)

    list_p = sub.add_parser("list", help="List scenarios")
    list_p.add_argument("--suite", choices=["core", "full"])
    list_p.add_argument("--profile", choices=["standard", "smoke"], default="standard")
    list_p.set_defaults(func=cmd_list)

    run_p = sub.add_parser("run", help="Run a scenario or suite")
    run_p.add_argument("--scenario")
    run_p.add_argument("--suite", choices=["core", "full"])
    run_p.add_argument("--profile", choices=["standard", "smoke"], default="smoke")
    run_p.add_argument("--runs", type=int)
    run_p.add_argument("--source", help="Paho source root containing src/paho")
    run_p.add_argument("--broker", help="External broker host:port")
    run_p.add_argument("--network", choices=sorted(PROFILES.keys()))
    run_p.add_argument("--load-profile", help="JSON from calibrate")
    run_p.add_argument("--output")
    run_p.add_argument("--seed", type=int, default=42)
    run_p.set_defaults(func=cmd_run)

    cal_p = sub.add_parser("calibrate", help="Create open-loop load profile from baseline capacity")
    cal_p.add_argument("--source", required=True)
    cal_p.add_argument("--output", required=True)
    cal_p.add_argument("--profile", choices=["standard", "smoke"], default="smoke")
    cal_p.set_defaults(func=cmd_calibrate)

    cmp_p = sub.add_parser("compare", help="ABBA compare two Paho sources")
    cmp_p.add_argument("--baseline-source", required=True)
    cmp_p.add_argument("--candidate-source", required=True)
    cmp_p.add_argument("--scenario", required=True)
    cmp_p.add_argument("--blocks", type=int, default=4)
    cmp_p.add_argument("--point-index", type=int, default=0)
    cmp_p.add_argument("--profile", choices=["standard", "smoke"], default="smoke")
    cmp_p.add_argument("--load-profile")
    cmp_p.add_argument("--output")
    cmp_p.set_defaults(func=cmd_compare)

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
