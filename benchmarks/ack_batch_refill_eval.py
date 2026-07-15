#!/usr/bin/env python3
"""Paired same-process evaluation for performance-audit project 15."""

from __future__ import annotations

import statistics
import sys
import time

import paho.mqtt.client as mqtt

sys.path.insert(0, "benchmarks")
from scenarios import _new_puback_refill_client  # noqa: E402


ACKS = 100
RUNS = 15
WARMUPS = 2


def _legacy_batch(client, max_packets):
    if client._read_buffer_pending() == 0:
        client._read_ahead_exhausted = False
    for _ in range(max_packets):
        if client._sock is None:
            return mqtt.MQTT_ERR_NO_CONN
        rc = client._packet_read_buffered()
        if rc > 0:
            return client._loop_rc_handle(rc)
        if rc == mqtt.MQTT_ERR_AGAIN:
            return mqtt.MQTT_ERR_SUCCESS
        if client._read_ahead_exhausted and client._read_buffer_pending() == 0:
            return mqtt.MQTT_ERR_SUCCESS
    return mqtt.MQTT_ERR_SUCCESS


def _legacy(client):
    rc = _legacy_batch(client, ACKS)
    if rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError("legacy ACK batch failed: {}".format(rc))


def _candidate(client):
    rc = client._loop_read_batch(ACKS)
    if rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError("candidate ACK batch failed: {}".format(rc))


def _validate(client):
    if client._inflight_messages != ACKS:
        raise RuntimeError("inflight window was not refilled")
    if len(client._out_messages) != 900:
        raise RuntimeError("unexpected outgoing message count")
    if len(client._out_packet) != ACKS:
        raise RuntimeError("unexpected promoted packet count")


def _measure(function):
    client = _new_puback_refill_client()
    started = time.perf_counter()
    function(client)
    elapsed = time.perf_counter() - started
    _validate(client)
    return elapsed


def _measure_single(candidate: bool):
    client = _new_puback_refill_client(total_messages=2, ack_count=1)
    started = time.perf_counter()
    if candidate:
        rc = client._loop_read_batch(ACKS)
    else:
        rc = _legacy_batch(client, ACKS)
    elapsed = time.perf_counter() - started
    if rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError("single ACK failed: {}".format(rc))
    if client._inflight_messages != 1 or len(client._out_messages) != 1:
        raise RuntimeError("single ACK did not refill the window")
    return elapsed


def main() -> int:
    for _ in range(WARMUPS):
        _measure(_legacy)
        _measure(_candidate)

    legacy_times = []
    candidate_times = []
    for run in range(RUNS):
        order = ((legacy_times, _legacy), (candidate_times, _candidate))
        if run % 2:
            order = tuple(reversed(order))
        for samples, function in order:
            samples.append(_measure(function))

    legacy_median = statistics.median(legacy_times)
    candidate_median = statistics.median(candidate_times)
    legacy_rate = ACKS / legacy_median
    candidate_rate = ACKS / candidate_median
    print("100-PUBACK saturated refill")
    print("  legacy:    {:,.0f} ACK/s [{:.6f}..{:.6f}]".format(
        legacy_rate, min(legacy_times), max(legacy_times)))
    print("  candidate: {:,.0f} ACK/s [{:.6f}..{:.6f}]".format(
        candidate_rate, min(candidate_times), max(candidate_times)))
    print("  delta:     {:+.1f}%".format((candidate_rate / legacy_rate - 1.0) * 100.0))

    single_legacy = []
    single_candidate = []
    for _ in range(200):
        single_legacy.append(_measure_single(False))
        single_candidate.append(_measure_single(True))
    legacy_single_median = statistics.median(single_legacy)
    candidate_single_median = statistics.median(single_candidate)
    print("single PUBACK refill")
    print("  legacy:    {:.2f} us".format(legacy_single_median * 1e6))
    print("  candidate: {:.2f} us ({:+.1f}%)".format(
        candidate_single_median * 1e6,
        (legacy_single_median / candidate_single_median - 1.0) * 100.0,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
