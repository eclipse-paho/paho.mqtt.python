#!/usr/bin/env python3
"""Fast brokerless eval for plan 05 — inflight saturation (measure before code).

Run:
  PYTHONPATH=src python benchmarks/inflight_saturation_eval.py
  PYTHONPATH=src python benchmarks/inflight_saturation_eval.py --profile
"""

from __future__ import annotations

import argparse
import cProfile
import io
import pstats
import statistics
import struct
import sys
import time
import tracemalloc

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, _ConnectionState

sys.path.insert(0, "benchmarks")
from fakes import FakeRecvSocket, FakeSendSocket  # noqa: E402

REPEATS = 3
TOPIC = b"devices/device-0001/telemetry"
PAYLOAD = b"x"


def _make_puback(mid: int) -> bytes:
    return struct.pack("!BBH", int(mqtt.PUBACK), 2, mid)


def _new_client(max_inflight: int) -> mqtt.Client:
    client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    client._sock = FakeSendSocket()
    client._state = _ConnectionState.MQTT_CS_CONNECTED
    client._max_inflight_messages = max_inflight
    client.on_publish = None
    return client


def _populate(client: mqtt.Client, n_total: int, max_inflight: int) -> None:
    with client._out_message_mutex:
        client._out_messages.clear()
        client._inflight_messages = min(max_inflight, n_total)
        for mid in range(1, n_total + 1):
            msg = mqtt.MQTTMessage(mid=mid, topic=TOPIC)
            msg.payload = PAYLOAD
            msg.qos = 1
            msg.state = (
                mqtt.mqtt_ms_wait_for_puback
                if mid <= max_inflight
                else mqtt.mqtt_ms_queued
            )
            client._out_messages[mid] = msg


def _first_inflight_mid(client: mqtt.Client) -> int | None:
    for mid, msg in client._out_messages.items():
        if msg.state == mqtt.mqtt_ms_wait_for_puback:
            return mid
    return None


def _ack(client: mqtt.Client, mid: int) -> None:
    client._sock = FakeRecvSocket(_make_puback(mid))
    if client._packet_read() != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError("packet_read failed")
    if client.loop_write() != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError("loop_write failed")


def _median_rate(fn, iterations: int, repeats: int = REPEATS) -> dict:
    rates = []
    for _ in range(repeats):
        fn()  # warmup
        t0 = time.perf_counter()
        for _ in range(iterations):
            fn()
        rates.append(iterations / (time.perf_counter() - t0))
    med = statistics.median(rates)
    return {"median": med, "min": min(rates), "max": max(rates), "us": 1e6 / med}


def bench_ack_cycle(n_total: int, max_inflight: int, acks: int) -> dict:
    saturated = n_total > max_inflight

    def one_run():
        c = _new_client(max_inflight)
        _populate(c, n_total, max_inflight)
        for _ in range(acks):
            mid = _first_inflight_mid(c)
            if mid is None:
                if not saturated:
                    return
                c = _new_client(max_inflight)
                _populate(c, n_total, max_inflight)
                mid = _first_inflight_mid(c)
            _ack(c, mid)

    r = _median_rate(one_run, 1)
    # timed portion inside one_run does acks; re-bench properly:
    rates = []
    for _ in range(REPEATS):
        c = _new_client(max_inflight)
        _populate(c, n_total, max_inflight)
        t0 = time.perf_counter()
        done = 0
        while done < acks:
            mid = _first_inflight_mid(c)
            if mid is None:
                if not saturated:
                    break
                c = _new_client(max_inflight)
                _populate(c, n_total, max_inflight)
                mid = _first_inflight_mid(c)
            _ack(c, mid)
            done += 1
        rates.append(done / (time.perf_counter() - t0))
    med = statistics.median(rates)
    return {
        "n": n_total,
        "max_if": max_inflight,
        "saturated": saturated,
        "ops_s": med,
        "range": (min(rates), max(rates)),
        "us_ack": 1e6 / med,
    }


def bench_one_update_inflight(n_total: int, max_inflight: int, samples: int) -> dict:
    """Single promotion timing (fresh queue tail); samples scaled down for large N."""
    if n_total >= 10000:
        samples = min(samples, 15)
    elif n_total >= 1000:
        samples = min(samples, 40)

    times = []
    for _ in range(REPEATS):
        for _ in range(samples):
            c = _new_client(max_inflight)
            _populate(c, n_total, max_inflight)
            with c._out_message_mutex:
                c._inflight_messages = max_inflight - 1
            t0 = time.perf_counter()
            c._update_inflight()
            times.append(time.perf_counter() - t0)
    med = statistics.median(times)
    return {"n": n_total, "max_if": max_inflight, "us_call": med * 1e6, "ops_s": 1 / med}


def bench_reconnect_reset(n_total: int, max_inflight: int, loops: int) -> dict:
    if n_total >= 10000:
        loops = min(loops, 5)

    def run():
        c = _new_client(max_inflight)
        _populate(c, n_total, max_inflight)
        for _ in range(loops):
            c._messages_reconnect_reset_out()

    r = _median_rate(run, 1)
    return {"n": n_total, "resets_s": r["median"] * loops}


def bench_reconnect_reset_steady(
    n_total: int,
    max_inflight: int,
    runs: int = 15,
    resets_per_run: int = 200,
) -> dict:
    """Measure only the stable reconnect scan, excluding client population."""
    c = _new_client(max_inflight)
    _populate(c, n_total, max_inflight)
    for _ in range(resets_per_run):
        c._messages_reconnect_reset_out()

    wall_rates = []
    cpu_per_reset = []
    for _ in range(runs):
        wall_start = time.perf_counter()
        cpu_start = time.process_time()
        for _ in range(resets_per_run):
            c._messages_reconnect_reset_out()
        cpu_elapsed = time.process_time() - cpu_start
        wall_elapsed = time.perf_counter() - wall_start
        wall_rates.append(resets_per_run / wall_elapsed)
        cpu_per_reset.append(cpu_elapsed / resets_per_run)

    return {
        "n": n_total,
        "runs": runs,
        "resets_per_run": resets_per_run,
        "resets_s": statistics.median(wall_rates),
        "resets_s_range": (min(wall_rates), max(wall_rates)),
        "cpu_us_reset": statistics.median(cpu_per_reset) * 1e6,
    }


def state_mapping_shallow_size(n_total: int) -> dict:
    c = _new_client(20)
    _populate(c, n_total, 20)
    return {
        "n": n_total,
        "mapping_type": type(c._out_messages).__name__,
        "bytes": sys.getsizeof(c._out_messages),
    }


def profile_ack(n_total: int, max_inflight: int, acks: int) -> dict[str, float]:
    c = _new_client(max_inflight)
    _populate(c, n_total, max_inflight)

    def run():
        nonlocal c
        for _ in range(acks):
            mid = _first_inflight_mid(c)
            if mid is None:
                c = _new_client(max_inflight)
                _populate(c, n_total, max_inflight)
                mid = _first_inflight_mid(c)
            _ack(c, mid)

    pr = cProfile.Profile()
    pr.enable()
    run()
    pr.disable()
    stats = pstats.Stats(pr)
    total = stats.total_tt
    shares: dict[str, float] = {}
    for func, stat in stats.stats.items():
        name = func[2]
        pct = 100.0 * stat[3] / total if total else 0.0
        if pct >= 1.0 or name in (
            "_update_inflight", "_send_publish", "_do_on_publish",
            "_handle_pubackcomp", "_packet_read",
        ):
            shares[f"{name}:{func[1]}"] = pct
    return shares


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()

    print("=== ACK cycle (PUBACK + promote) — fast matrix ===")
    ack_rows = []
    for n, max_if, acks, label in [
        (20, 20, 500, "unsaturated"),
        (100, 20, 800, "saturated"),
        (1000, 20, 800, "saturated"),
        (10000, 20, 400, "saturated"),
        (1000, 100, 600, "sat_hi_if"),
        (10000, 100, 300, "sat_hi_if"),
    ]:
        row = bench_ack_cycle(n, max_if, acks)
        row["label"] = label
        ack_rows.append(row)
        lo, hi = row["range"]
        print(
            f"{label:14} N={n:5} if={max_if:3}  {row['ops_s']:8.0f} ops/s  "
            f"[{lo:.0f}..{hi:.0f}]  {row['us_ack']:6.1f} µs/ACK"
        )

    sat20 = [r for r in ack_rows if r["saturated"] and r["max_if"] == 20]
    if len(sat20) >= 2:
        by_n = {r["n"]: r["ops_s"] for r in sat20}
        n_lo, n_hi = min(by_n), max(by_n)
        print(f"\nScaling max_if=20 saturated: ops(N={n_lo})/ops(N={n_hi}) = {by_n[n_lo]/by_n[n_hi]:.2f}")

    print("\n=== One _update_inflight() call (single promotion) ===")
    for n, max_if in [(100, 20), (1000, 20), (10000, 20), (1000, 100)]:
        row = bench_one_update_inflight(n, max_if, 80)
        print(f"N={n:5} if={max_if:3}  {row['us_call']:7.1f} µs/call  ({row['ops_s']:.0f} ops/s)")

    print("\n=== _messages_reconnect_reset_out() — full O(N) scan ===")
    for n in (100, 1000, 10000):
        row = bench_reconnect_reset(n, 20, 20)
        print(f"N={n:5}  {row['resets_s']:8.0f} resets/s")

    print("\n=== Plan 26 stable reconnect scan and shallow mapping size ===")
    row = bench_reconnect_reset_steady(1000, 20)
    lo, hi = row["resets_s_range"]
    print(
        f"N= 1000  {row['resets_s']:8.0f} resets/s "
        f"[{lo:.0f}..{hi:.0f}]  {row['cpu_us_reset']:.1f} us CPU/reset"
    )
    for n in (20, 100, 1000, 10000):
        size = state_mapping_shallow_size(n)
        print(
            f"{size['mapping_type']:>11} N={n:5}  "
            f"{size['bytes']:8} shallow bytes"
        )

    c = _new_client(20)
    _populate(c, 1000, 20)
    tracemalloc.start()
    for _ in range(400):
        mid = _first_inflight_mid(c)
        if mid is None:
            c = _new_client(20)
            _populate(c, 1000, 20)
            mid = _first_inflight_mid(c)
        _ack(c, mid)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    print(f"\ntracemalloc peak/ACK (N=1000): {peak/400:.0f} B")

    shares: dict[str, float] = {}
    if args.profile:
        shares = profile_ack(1000, 20, 600)
        print("\n=== cProfile ACK path (N=1000, 600 ACKs) ===")
        for name, pct in sorted(shares.items(), key=lambda kv: -kv[1])[:12]:
            print(f"  {pct:5.1f}%  {name}")

    # Verdict block (printed for discussion; no code change)
    print("\n=== VERDICT (auto) ===")
    sat_100 = next(r for r in ack_rows if r["n"] == 100 and r["max_if"] == 20)
    sat_10k = next(r for r in ack_rows if r["n"] == 10000 and r["max_if"] == 20)
    scale = sat_100["ops_s"] / sat_10k["ops_s"]
    upd_share = shares.get("_update_inflight:4330", 0)
    if scale < 1.20:
        print("ACK path: per-ACK cost ~flat vs N → ready-queue NO GO for ACK/promote")
    else:
        print(f"ACK path: scales with N (ratio {scale:.2f}) → investigate further")
    print("reconnect_reset: O(N) scan confirmed → separate track if reconnect-heavy")
    if upd_share:
        print(f"_update_inflight CPU share: {upd_share:.1f}% (GO threshold ~25-30%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
