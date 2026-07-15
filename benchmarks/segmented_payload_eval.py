#!/usr/bin/env python3
"""Construction-time and allocation evaluation for performance-audit project 18."""

from __future__ import annotations

import gc
import statistics
import threading
import time
import tracemalloc

import paho.mqtt.client as mqtt

from fakes import FakeSendSocket


RUNS = 15
WARMUPS = 2
TOPIC = b"bench/large-payload"


def _measure(payload):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client._sock = FakeSendSocket()
    client._thread = threading.Thread(target=lambda: None)
    gc.collect()
    tracemalloc.start()
    started = time.perf_counter()
    rc = client._send_publish(1, TOPIC, payload, qos=1)
    elapsed = time.perf_counter() - started
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    if rc != mqtt.MQTT_ERR_SUCCESS or len(client._out_packet) != 1:
        raise RuntimeError("large payload was not queued")
    return elapsed, peak


def main():
    for size in (128, 1024 * 1024, 64 * 1024 * 1024):
        payload = b"x" * size
        for _ in range(WARMUPS):
            _measure(payload)
        samples = [_measure(payload) for _ in range(RUNS)]
        elapsed = [sample[0] for sample in samples]
        peaks = [sample[1] for sample in samples]
        print("{} bytes".format(size))
        print("  median: {:.3f} ms [{:.3f}..{:.3f}]".format(
            statistics.median(elapsed) * 1000,
            min(elapsed) * 1000,
            max(elapsed) * 1000,
        ))
        print("  peak:   {:,.0f} bytes".format(statistics.median(peaks)))


if __name__ == "__main__":
    main()
