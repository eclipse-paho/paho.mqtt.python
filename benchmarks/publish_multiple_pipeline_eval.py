#!/usr/bin/env python3
"""Brokerless event-model evaluation for performance-audit project 23."""

from __future__ import annotations

import argparse
import gc
import heapq
import json
import statistics
import sys
import time
import tracemalloc
from pathlib import Path


class _Info:
    def __init__(self, mid, rc):
        self.mid = mid
        self.rc = rc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument(
        "--real-broker",
        help="also time the public helper against HOST:PORT",
    )
    args = parser.parse_args()

    source = Path(args.source).resolve()
    sys.path.insert(0, str(source / "src"))

    import paho.mqtt.publish as publish_module  # noqa: PLC0415

    scenarios = (
        ("one_qos1_zero", 1, "qos1", 0.0),
        ("hundred_qos1_zero", 100, "qos1", 0.0),
        ("hundred_qos1_20ms", 100, "qos1", 0.020),
        ("hundred_qos1_100ms", 100, "qos1", 0.100),
        ("hundred_mixed_20ms", 100, "mixed", 0.020),
        ("thousand_qos1_20ms", 1000, "qos1", 0.020),
    )

    def messages(count, qos_mode):
        result = []
        for index in range(count):
            qos = 1 if qos_mode == "qos1" else index % 3
            result.append({
                "topic": "audit/{:04d}".format(index),
                "payload": b"x",
                "qos": qos,
            })
        return result

    def run_once(count, qos_mode, ack_delay, measure_memory=False):
        expected = messages(count, qos_mode)
        instances = []

        class FakeClient:
            def __init__(self, callback_api_version, client_id, userdata, protocol, transport):
                self._userdata = userdata
                self.on_connect = None
                self.on_publish = None
                self.virtual_time = 0.0
                self.events = []
                self.sequence = 0
                self.next_mid = 0
                self.wire_outstanding = 0
                self.peak_outstanding = 0
                self.publish_order = []
                self.callback_count = 0
                self.disconnect_count = 0
                self.disconnected = False
                instances.append(self)

            def enable_logger(self):
                return None

            def connect(self, hostname, port, keepalive):
                return 0

            def publish(self, topic, payload=None, qos=0, retain=False):
                self.next_mid += 1
                mid = self.next_mid
                self.publish_order.append(topic)
                self.wire_outstanding += 1
                self.peak_outstanding = max(
                    self.peak_outstanding, self.wire_outstanding
                )
                delay = 0.0 if qos == 0 else ack_delay
                heapq.heappush(
                    self.events,
                    (self.virtual_time + delay, self.sequence, mid),
                )
                self.sequence += 1
                return _Info(mid, 0)

            def disconnect(self):
                self.disconnect_count += 1
                self.disconnected = True
                return 0

            def loop_forever(self):
                self.on_connect(self, self._userdata, {}, 0, None)
                while self.events and not self.disconnected:
                    due, _sequence, mid = heapq.heappop(self.events)
                    self.virtual_time = due
                    self.wire_outstanding -= 1
                    self.callback_count += 1
                    self.on_publish(self, self._userdata, mid, None, None)
                if not self.disconnected:
                    raise RuntimeError("helper did not disconnect")
                if self.events or self.wire_outstanding:
                    raise RuntimeError("helper disconnected with messages outstanding")

        original_client = publish_module.paho.Client
        publish_module.paho.Client = FakeClient
        gc.collect()
        if measure_memory:
            tracemalloc.start()
            tracemalloc.reset_peak()
        gc_was_enabled = gc.isenabled()
        gc.disable()
        cpu_started = time.process_time()
        started = time.perf_counter()
        try:
            publish_module.multiple(expected)
        finally:
            publish_module.paho.Client = original_client
            if gc_was_enabled:
                gc.enable()
        elapsed = time.perf_counter() - started
        cpu_elapsed = time.process_time() - cpu_started
        peak_bytes = 0
        if measure_memory:
            _current_bytes, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()

        if len(instances) != 1:
            raise RuntimeError("expected one helper client")
        client = instances[0]
        expected_order = [message["topic"] for message in expected]
        if client.publish_order != expected_order:
            raise RuntimeError("input order changed")
        if client.callback_count != count:
            raise RuntimeError("message completion count mismatch")
        if client.disconnect_count != 1:
            raise RuntimeError("disconnect count mismatch")
        return {
            "elapsed_s": elapsed,
            "cpu_s": cpu_elapsed,
            "virtual_wall_s": client.virtual_time,
            "peak_outstanding": client.peak_outstanding,
            "peak_bytes": peak_bytes,
        }

    output = {}
    for name, count, qos_mode, ack_delay in scenarios:
        for _ in range(args.warmups):
            run_once(count, qos_mode, ack_delay)
        samples = [
            run_once(count, qos_mode, ack_delay) for _ in range(args.runs)
        ]
        memory = run_once(count, qos_mode, ack_delay, measure_memory=True)
        elapsed = [sample["elapsed_s"] for sample in samples]
        cpu = [sample["cpu_s"] for sample in samples]
        output[name] = {
            "messages": count,
            "qos_mode": qos_mode,
            "ack_delay_ms": ack_delay * 1000.0,
            "cpu_median_ms": statistics.median(cpu) * 1000.0,
            "elapsed_median_ms": statistics.median(elapsed) * 1000.0,
            "elapsed_min_ms": min(elapsed) * 1000.0,
            "elapsed_max_ms": max(elapsed) * 1000.0,
            "virtual_wall_ms": samples[0]["virtual_wall_s"] * 1000.0,
            "virtual_msgs_per_s": (
                count / samples[0]["virtual_wall_s"]
                if samples[0]["virtual_wall_s"] > 0
                else None
            ),
            "peak_outstanding": samples[0]["peak_outstanding"],
            "tracemalloc_peak_bytes": memory["peak_bytes"],
        }

    result = {
        "source": str(source),
        "runs": args.runs,
        "warmups": args.warmups,
        "scenarios": output,
    }

    if args.real_broker:
        host, port_text = args.real_broker.rsplit(":", 1)
        port = int(port_text)
        real_output = {}

        def run_real_once(count, qos):
            gc.collect()
            gc_was_enabled = gc.isenabled()
            gc.disable()
            started_cpu = time.process_time()
            started = time.perf_counter()
            try:
                publish_module.multiple(
                    messages(count, "qos1" if qos == 1 else "mixed")
                    if qos == 1
                    else [
                        {
                            "topic": "audit/{:04d}".format(index),
                            "payload": b"x",
                            "qos": qos,
                        }
                        for index in range(count)
                    ],
                    hostname=host,
                    port=port,
                )
                return time.perf_counter() - started, time.process_time() - started_cpu
            finally:
                if gc_was_enabled:
                    gc.enable()

        for name, count, qos in (
            ("one_qos1_loopback", 1, 1),
            ("hundred_qos0_loopback", 100, 0),
            ("hundred_qos1_loopback", 100, 1),
            ("hundred_qos2_loopback", 100, 2),
        ):
            for _ in range(args.warmups):
                run_real_once(count, qos)
            real_samples = [run_real_once(count, qos) for _ in range(args.runs)]
            elapsed = [sample[0] for sample in real_samples]
            cpu = [sample[1] for sample in real_samples]
            real_output[name] = {
                "messages": count,
                "qos": qos,
                "elapsed_median_ms": statistics.median(elapsed) * 1000.0,
                "elapsed_min_ms": min(elapsed) * 1000.0,
                "elapsed_max_ms": max(elapsed) * 1000.0,
                "cpu_median_ms": statistics.median(cpu) * 1000.0,
                "msgs_per_s": count / statistics.median(elapsed),
            }
        result["real_broker"] = {
            "endpoint": args.real_broker,
            "scenarios": real_output,
        }

    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
