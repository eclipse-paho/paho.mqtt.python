#!/usr/bin/env python3
"""Measure idle threaded-loop wakeups and stop latency without a broker."""

from __future__ import annotations

import argparse
import json
import resource
import socket
import sys
import threading
import time
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--duration", type=float, default=2.2)
    parser.add_argument("--keepalive", type=int, default=60)
    parser.add_argument("--clients", type=int, default=1)
    parser.add_argument("--latency-probes", type=int, default=0)
    args = parser.parse_args()

    source = Path(args.source).resolve()
    sys.path.insert(0, str(source / "src"))

    import paho.mqtt.client as mqtt  # noqa: PLC0415
    from paho.mqtt.enums import CallbackAPIVersion, _ConnectionState  # noqa: PLC0415

    real_select = mqtt.select.select
    wakeups = 0

    def counted_select(*select_args, **select_kwargs):
        nonlocal wakeups
        result = real_select(*select_args, **select_kwargs)
        wakeups += 1
        return result

    clients = []
    sockets = []
    for _ in range(args.clients):
        network_socket, peer = socket.socketpair()
        client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        client._sock = network_socket
        client._state = _ConnectionState.MQTT_CS_CONNECTED
        client._keepalive = args.keepalive
        clients.append(client)
        sockets.append((network_socket, peer))
    mqtt.select.select = counted_select
    try:
        usage_before = resource.getrusage(resource.RUSAGE_SELF)
        cpu_before = time.process_time()
        for client in clients:
            client.loop_start()
        time.sleep(args.duration)
        stop_started = time.perf_counter()
        # Signal all clients before joining any one thread so the baseline pays
        # at most one polling interval rather than one interval per client.
        for client in clients:
            client._thread_terminate = True
            event = getattr(client, "_thread_terminate_event", None)
            if event is not None:
                event.set()
            wake = getattr(client, "_wake_thread", None)
            if wake is not None:
                wake()
        for client in clients:
            if client._thread is not None:
                client._thread.join()
        stop_ms = (time.perf_counter() - stop_started) * 1000.0
        cpu_s = time.process_time() - cpu_before
        usage_after = resource.getrusage(resource.RUSAGE_SELF)
    finally:
        mqtt.select.select = real_select
        for network_socket, peer in sockets:
            network_socket.close()
            peer.close()

    reconnect_client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    reconnect_client._reconnect_min_delay = 60
    reconnect_client._reconnect_max_delay = 60
    reconnect_client._thread = threading.Thread(target=reconnect_client._reconnect_wait)
    reconnect_client._thread.start()
    time.sleep(0.02)
    reconnect_stop_started = time.perf_counter()
    reconnect_client.loop_stop()
    reconnect_stop_ms = (time.perf_counter() - reconnect_stop_started) * 1000.0

    publish_latencies_ms = []
    for probe in range(args.latency_probes):
        network_socket, peer = socket.socketpair()
        peer.settimeout(2.0)
        latency_client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"deadline-probe-{probe}",
        )
        latency_client._sock = network_socket
        latency_client._state = _ConnectionState.MQTT_CS_CONNECTED
        latency_client._keepalive = args.keepalive
        latency_client.loop_start()
        time.sleep(1.1)
        started = time.perf_counter()
        latency_client.publish("deadline/probe", b"x", qos=0)
        peer.recv(4096)
        publish_latencies_ms.append((time.perf_counter() - started) * 1000.0)
        latency_client.loop_stop()
        network_socket.close()
        peer.close()

    publish_latencies_ms.sort()
    p95_index = max(0, int(0.95 * len(publish_latencies_ms) + 0.999999) - 1)

    print(json.dumps({
        "source": str(source),
        "duration_s": args.duration,
        "clients": args.clients,
        "keepalive_s": args.keepalive,
        "selector_returns": wakeups,
        "stop_ms": stop_ms,
        "process_cpu_s": cpu_s,
        "voluntary_context_switches": usage_after.ru_nvcsw - usage_before.ru_nvcsw,
        "involuntary_context_switches": usage_after.ru_nivcsw - usage_before.ru_nivcsw,
        "reconnect_stop_ms": reconnect_stop_ms,
        "publish_latency_median_ms": (
            publish_latencies_ms[len(publish_latencies_ms) // 2]
            if publish_latencies_ms else None
        ),
        "publish_latency_p95_ms": publish_latencies_ms[p95_index] if publish_latencies_ms else None,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
