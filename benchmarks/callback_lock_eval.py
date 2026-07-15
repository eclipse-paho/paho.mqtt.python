#!/usr/bin/env python3
"""Isolated evaluation for performance-audit project 22.

The script deliberately loads the client from ``--source`` so the benchmark
driver stays fixed while baseline and candidate implementations vary.
"""

from __future__ import annotations

import argparse
import json
import statistics
import struct
import sys
import threading
import time
import tracemalloc
from pathlib import Path


DELAYS_MS = (0, 1, 10, 200)


def _percentile(samples: list[float], percentile: float) -> float:
    ordered = sorted(samples)
    index = max(0, int(percentile * len(ordered) + 0.999999) - 1)
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--runs", type=int, default=15)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--only", choices=("all", "delays", "batches"), default="all")
    args = parser.parse_args()

    source = Path(args.source).resolve()
    sys.path.insert(0, str(source / "src"))

    import paho.mqtt.client as mqtt  # noqa: PLC0415
    from paho.mqtt.enums import CallbackAPIVersion  # noqa: PLC0415

    class ConnectedSocket:
        """Marker socket; threaded packet queueing does not write to it."""

        def send(self, data: bytes | bytearray) -> int:
            return len(data)

        def close(self) -> None:
            return None

    def prepare_ack(client, mid: int) -> None:
        client._in_packet.remaining_length = 2
        client._in_packet.packet = struct.pack("!H", mid)

    def add_message(client, mid: int) -> mqtt.MQTTMessage:
        message = mqtt.MQTTMessage(mid=mid, topic=b"audit/out")
        message.qos = 1
        message.state = mqtt.mqtt_ms_wait_for_puback
        client._out_messages[mid] = message
        return message

    def legacy_v3_puback(client) -> int:
        """Plan-10 no-callback control, kept local for same-process pairing."""
        if client._protocol == mqtt.MQTTv5:
            if client._in_packet.remaining_length < 2:
                return int(mqtt.MQTT_ERR_PROTOCOL)
        elif client._in_packet.remaining_length != 2:
            return int(mqtt.MQTT_ERR_PROTOCOL)

        packet_type_enum = mqtt.PUBACK
        _packet_type = packet_type_enum.value >> 4
        mid = struct.unpack_from("!H", client._in_packet.packet, 0)[0]
        client._easy_log(mqtt.MQTT_LOG_DEBUG, "Received PUBACK (Mid: %d)", mid)
        with client._out_message_mutex:
            if mid not in client._out_messages:
                return int(mqtt.MQTT_ERR_SUCCESS)
            with client._callback_mutex:
                on_publish = client._on_publish
            if on_publish is None:
                return int(client._complete_outgoing_publish(mid))
        raise RuntimeError("legacy control only supports the no-callback path")

    def concurrent_publish(delay_ms: int) -> dict[str, float | bool]:
        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        client._sock = ConnectedSocket()
        client._max_inflight_messages = 1
        client._inflight_messages = 1
        completed = add_message(client, 1)
        client._last_mid = 1
        entered = threading.Event()
        published_during_callback: list[bool] = []

        def on_publish(*_args: object) -> None:
            published_during_callback.append(completed.info.is_published())
            entered.set()
            time.sleep(delay_ms / 1000.0)

        client.on_publish = on_publish
        prepare_ack(client, 1)
        ack_result: list[int] = []

        def acknowledge() -> None:
            ack_result.append(int(client._handle_pubackcomp("PUBACK")))

        ack_thread = threading.Thread(target=acknowledge)
        # Keep publish() on its normal threaded queueing path without touching
        # a real socket; the benchmark is about the message-state mutex.
        client._thread = ack_thread
        ack_started = time.perf_counter()
        ack_thread.start()
        if not entered.wait(1.0):
            raise RuntimeError("on_publish was not entered")

        publish_started = time.perf_counter()
        queued = client.publish("audit/concurrent", b"x", qos=1)
        publish_ms = (time.perf_counter() - publish_started) * 1000.0
        ack_thread.join(2.0)
        if ack_thread.is_alive():
            raise RuntimeError("ACK thread deadlocked")
        ack_ms = (time.perf_counter() - ack_started) * 1000.0
        if ack_result != [int(mqtt.MQTT_ERR_SUCCESS)]:
            raise RuntimeError("ACK failed: {!r}".format(ack_result))
        if queued.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("concurrent publish failed: {}".format(queued.rc))
        if not completed.info.is_published():
            raise RuntimeError("ACKed MQTTMessageInfo was not published")
        if published_during_callback != [False]:
            raise RuntimeError("MQTTMessageInfo completed before its callback")
        if client._inflight_messages != 1:
            raise RuntimeError("inflight reservation/refill mismatch")
        return {
            "publish_ms": publish_ms,
            "ack_ms": ack_ms,
            "published_during_callback": published_during_callback[0],
        }

    def ack_batch(
        with_callback: bool,
        measure_memory: bool = False,
        legacy_no_callback: bool = False,
    ) -> dict[str, float]:
        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        client._max_inflight_messages = 0
        client._inflight_messages = args.batch_size
        if with_callback:
            client.on_publish = lambda *_args: None
        for mid in range(1, args.batch_size + 1):
            add_message(client, mid)

        if measure_memory:
            tracemalloc.start()
            tracemalloc.reset_peak()
        cpu_started = time.process_time()
        started = time.perf_counter()
        for mid in range(1, args.batch_size + 1):
            prepare_ack(client, mid)
            if legacy_no_callback:
                rc = legacy_v3_puback(client)
            else:
                rc = client._handle_pubackcomp("PUBACK")
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError("batch ACK failed: {}".format(rc))
        elapsed = time.perf_counter() - started
        cpu_elapsed = time.process_time() - cpu_started
        peak_bytes = 0
        if measure_memory:
            _current_bytes, peak_bytes = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        if client._out_messages or client._inflight_messages != 0:
            raise RuntimeError("batch did not complete cleanly")
        return {
            "elapsed_s": elapsed,
            "cpu_s": cpu_elapsed,
            "peak_bytes": float(peak_bytes),
        }

    # Warm every shape before recording. The 200-ms case is intentionally not
    # shortened: it is the acceptance workload, not a synthetic extrapolation.
    for _ in range(args.warmups):
        if args.only in ("all", "delays"):
            for delay_ms in DELAYS_MS:
                concurrent_publish(delay_ms)
        if args.only in ("all", "batches"):
            ack_batch(False)
            ack_batch(True)

    delays: dict[str, dict[str, float]] = {}
    if args.only in ("all", "delays"):
        for delay_ms in DELAYS_MS:
            publish_samples = []
            ack_samples = []
            for _ in range(args.runs):
                sample = concurrent_publish(delay_ms)
                publish_samples.append(float(sample["publish_ms"]))
                ack_samples.append(float(sample["ack_ms"]))
            delays[str(delay_ms)] = {
                "publish_p50_ms": statistics.median(publish_samples),
                "publish_p95_ms": _percentile(publish_samples, 0.95),
                "publish_p99_ms": _percentile(publish_samples, 0.99),
                "publish_min_ms": min(publish_samples),
                "publish_max_ms": max(publish_samples),
                "ack_median_ms": statistics.median(ack_samples),
            }

    batches: dict[str, dict[str, float]] = {}
    if args.only in ("all", "batches"):
        for with_callback, name in ((False, "no_callback"), (True, "noop_callback")):
            samples = [ack_batch(with_callback) for _ in range(args.runs)]
            elapsed_samples = [sample["elapsed_s"] for sample in samples]
            cpu_samples = [sample["cpu_s"] for sample in samples]
            median_elapsed = statistics.median(elapsed_samples)
            memory_sample = ack_batch(with_callback, measure_memory=True)
            batches[name] = {
                "ack_per_second": args.batch_size / median_elapsed,
                "median_ms": median_elapsed * 1000.0,
                "cpu_median_ms": statistics.median(cpu_samples) * 1000.0,
                "min_ms": min(elapsed_samples) * 1000.0,
                "max_ms": max(elapsed_samples) * 1000.0,
                "tracemalloc_peak_bytes": memory_sample["peak_bytes"],
            }

        # Cross-process CPU frequency changes can dwarf the tiny no-callback
        # handler. Pair the retained plan-10 logic and current handler in the
        # same interpreter to distinguish code cost from thermal/order drift.
        current_samples = []
        legacy_samples = []
        for run in range(args.runs):
            order = ((current_samples, False), (legacy_samples, True))
            if run % 2:
                order = tuple(reversed(order))
            for samples, legacy in order:
                samples.append(
                    ack_batch(False, legacy_no_callback=legacy)["cpu_s"]
                )
        current_cpu = statistics.median(current_samples)
        legacy_cpu = statistics.median(legacy_samples)
        batches["no_callback_same_process_control"] = {
            "current_cpu_median_ms": current_cpu * 1000.0,
            "legacy_cpu_median_ms": legacy_cpu * 1000.0,
            "current_vs_legacy_pct": (legacy_cpu / current_cpu - 1.0) * 100.0,
        }

    print(json.dumps({
        "source": str(source),
        "runs": args.runs,
        "warmups": args.warmups,
        "batch_size": args.batch_size,
        "delays": delays,
        "batches": batches,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
