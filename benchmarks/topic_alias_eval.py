#!/usr/bin/env python3
"""Brokerless evaluation for performance-audit project 24.

The benchmark driver is kept fixed while ``--source`` selects the client
implementation under test.  A synthetic successful MQTT v5 CONNACK advertises
the requested Topic Alias Maximum, then public ``publish()`` calls are queued
without involving socket or broker scheduling.
"""

from __future__ import annotations

import argparse
import gc
import json
import statistics
import sys
import threading
import time
import tracemalloc
from pathlib import Path


SCENARIOS = ("repeated-1", "recurring-8", "recurring-16", "recurring-17",
             "recurring-1000", "all-distinct")


class MarkerSocket:
    def send(self, data):
        return len(data)

    def close(self):
        return None


def percentile(samples, fraction):
    ordered = sorted(samples)
    index = max(0, int(fraction * len(ordered) + 0.999999) - 1)
    return ordered[index]


def make_topic(index, length, width):
    suffix = "/{:0{width}d}".format(index, width=width)
    prefix_length = length - len(suffix)
    if prefix_length < 1:
        raise ValueError("topic length is too short for the unique suffix")
    return "t" * prefix_length + suffix


def topics_for(scenario, count, topic_length):
    if scenario == "all-distinct":
        width = max(1, len(str(count - 1)))
        return [make_topic(index, topic_length, width) for index in range(count)]
    cardinality = int(scenario.rsplit("-", 1)[1])
    width = max(1, len(str(cardinality - 1)))
    recurring = [make_topic(index, topic_length, width) for index in range(cardinality)]
    return [recurring[index % cardinality] for index in range(count)]


def prepare_client(mqtt, Properties, PacketTypes, alias_limit):
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5,
    )
    client._sock = MarkerSocket()
    # Keep _packet_queue() on its normal threaded queueing path without
    # starting a worker that would add scheduler noise to construction cost.
    client._thread = threading.current_thread()
    properties = Properties(PacketTypes.CONNACK)
    properties.TopicAliasMaximum = alias_limit
    packet = bytes((0, 0)) + bytes(properties.pack())
    client._in_packet.remaining_length = len(packet)
    client._in_packet.packet = packet
    rc = client._handle_connack()
    if rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError("synthetic CONNACK failed: {}".format(rc))
    return client


def run_once(mqtt, Properties, PacketTypes, topics, payload, alias_limit,
             measure_memory=False):
    gc.collect()
    client = prepare_client(mqtt, Properties, PacketTypes, alias_limit)
    if measure_memory:
        tracemalloc.start()
        tracemalloc.reset_peak()
    gc_enabled = gc.isenabled()
    gc.disable()
    cpu_started = time.process_time()
    started = time.perf_counter()
    try:
        for topic in topics:
            result = client.publish(topic, payload, qos=0)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError("publish failed: {}".format(result.rc))
    finally:
        elapsed = time.perf_counter() - started
        cpu_elapsed = time.process_time() - cpu_started
        if gc_enabled:
            gc.enable()

    packet_count = len(client._out_packet)
    wire_bytes = sum(packet["to_process"] for packet in client._out_packet)
    alias_table = getattr(client, "_outbound_topic_aliases", {})
    alias_count = len(alias_table)
    client._out_packet.clear()
    del result
    gc.collect()
    retained_bytes = 0
    peak_bytes = 0
    if measure_memory:
        retained_bytes, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
    client._sock = None
    client._thread = None
    if packet_count != len(topics):
        raise RuntimeError("queued packet count mismatch")
    return {
        "elapsed_s": elapsed,
        "cpu_s": cpu_elapsed,
        "wire_bytes": wire_bytes,
        "alias_count": alias_count,
        "retained_bytes": retained_bytes,
        "peak_bytes": peak_bytes,
    }


def summarize(samples, message_count, memory_sample):
    elapsed = [sample["elapsed_s"] for sample in samples]
    cpu = [sample["cpu_s"] for sample in samples]
    wire = [sample["wire_bytes"] for sample in samples]
    median_elapsed = statistics.median(elapsed)
    return {
        "messages_per_second": message_count / median_elapsed,
        "wall_median_ms": median_elapsed * 1000.0,
        "wall_min_ms": min(elapsed) * 1000.0,
        "wall_max_ms": max(elapsed) * 1000.0,
        "wall_p95_ms": percentile(elapsed, 0.95) * 1000.0,
        "cpu_median_ms": statistics.median(cpu) * 1000.0,
        "wire_bytes_per_message": statistics.median(wire) / message_count,
        "alias_count": memory_sample["alias_count"],
        "retained_bytes_after_queue_clear": memory_sample["retained_bytes"],
        "tracemalloc_peak_bytes": memory_sample["peak_bytes"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--runs", type=int, default=15)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--messages", type=int, default=10_000)
    parser.add_argument("--topic-length", type=int, default=128)
    parser.add_argument("--payload-size", type=int, default=16)
    parser.add_argument("--alias-limit", type=int, default=16)
    parser.add_argument("--scenario", choices=("all",) + SCENARIOS, default="all")
    parser.add_argument("--output")
    args = parser.parse_args()

    source = Path(args.source).resolve()
    sys.path.insert(0, str(source / "src"))
    import paho.mqtt.client as mqtt  # noqa: PLC0415
    from paho.mqtt.packettypes import PacketTypes  # noqa: PLC0415
    from paho.mqtt.properties import Properties  # noqa: PLC0415

    selected = SCENARIOS if args.scenario == "all" else (args.scenario,)
    payload = b"x" * args.payload_size
    results = {}
    for scenario in selected:
        topics = topics_for(scenario, args.messages, args.topic_length)
        for _ in range(args.warmups):
            run_once(mqtt, Properties, PacketTypes, topics, payload, args.alias_limit)
        samples = [
            run_once(mqtt, Properties, PacketTypes, topics, payload, args.alias_limit)
            for _ in range(args.runs)
        ]
        memory_sample = run_once(
            mqtt, Properties, PacketTypes, topics, payload, args.alias_limit,
            measure_memory=True,
        )
        results[scenario] = summarize(samples, args.messages, memory_sample)

    report = {
        "source": str(source),
        "runs": args.runs,
        "warmups": args.warmups,
        "messages": args.messages,
        "topic_length": args.topic_length,
        "payload_size": args.payload_size,
        "alias_limit": args.alias_limit,
        "scenarios": results,
    }
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
