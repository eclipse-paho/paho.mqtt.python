#!/usr/bin/env python3
"""Paired same-process evaluation for performance-audit project 14."""

from __future__ import annotations

import gc
import statistics
import socket
import ssl
import sys
import threading
import time
import tracemalloc

import paho.mqtt.client as mqtt

sys.path.insert(0, "benchmarks")
from fakes import NonBlockingRecvSocket  # noqa: E402
from scenarios import PUBLISH_V3_QOS0_SMALL, _new_client  # noqa: E402


ITERATIONS = 10_000
RUNS = 15
WARMUPS = 2
TLS_ITERATIONS = 3_000


class _CountingTLSSocket:
    def __init__(self, sock):
        self._sock = sock
        self.recv_calls = 0

    def recv(self, size):
        self.recv_calls += 1
        return self._sock.recv(size)

    def send(self, data):
        return self._sock.send(data)

    def pending(self):
        return self._sock.pending()

    def close(self):
        return self._sock.close()


def _new_burst_client(iterations: int):
    client = _new_client(mqtt.MQTTv311)
    delivered = [0]
    client.on_message = lambda *args: delivered.__setitem__(0, delivered[0] + 1)
    sock = NonBlockingRecvSocket(PUBLISH_V3_QOS0_SMALL * iterations)
    client._sock = sock
    return client, delivered, sock


def _legacy_internal_batch(iterations: int):
    client, delivered, sock = _new_burst_client(iterations)
    batch_calls = 0
    while delivered[0] < iterations:
        if client._read_buffer_pending() == 0:
            client._read_ahead_exhausted = False
        for _ in range(100):
            rc = client._packet_read(read_ahead=True)
            if rc > 0:
                raise RuntimeError("legacy packet read failed: {}".format(rc))
            if rc == mqtt.MQTT_ERR_AGAIN:
                break
            if client._read_ahead_exhausted and client._read_buffer_pending() == 0:
                break
        batch_calls += 1
    return sock.recv_calls, batch_calls


def _candidate_internal_batch(iterations: int):
    client, delivered, sock = _new_burst_client(iterations)
    batch_calls = 0
    while delivered[0] < iterations:
        rc = client._loop_read_batch(100)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("candidate batch failed: {}".format(rc))
        batch_calls += 1
    return sock.recv_calls, batch_calls


def _new_tls_burst_client(iterations: int):
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain("tests/ssl/server.crt", "tests/ssl/server.key")
    client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    client_context.check_hostname = False
    client_context.verify_mode = ssl.CERT_NONE
    server_raw, client_raw = socket.socketpair()
    data = PUBLISH_V3_QOS0_SMALL * iterations
    server_errors = []

    def serve():
        try:
            with server_context.wrap_socket(server_raw, server_side=True) as server_sock:
                server_sock.sendall(data)
        except Exception as err:  # pragma: no cover - reported in benchmark output
            server_errors.append(err)

    server_thread = threading.Thread(target=serve, name="bench-ingress-tls")
    server_thread.start()
    client_sock = client_context.wrap_socket(client_raw, server_hostname="localhost")
    client, delivered, _ = _new_burst_client(0)
    counting_sock = _CountingTLSSocket(client_sock)
    client._sock = counting_sock
    return client, delivered, counting_sock, server_thread, server_errors


def _tls_internal_batch(iterations: int, candidate: bool):
    client, delivered, sock, server_thread, server_errors = _new_tls_burst_client(iterations)
    batch_calls = 0
    started = time.perf_counter()
    while delivered[0] < iterations:
        if candidate:
            rc = client._loop_read_batch(100)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError("candidate TLS batch failed: {}".format(rc))
        else:
            if client._read_buffer_pending() == 0:
                client._read_ahead_exhausted = False
            for _ in range(100):
                rc = client._packet_read(read_ahead=True)
                if rc > 0:
                    raise RuntimeError("legacy TLS packet read failed: {}".format(rc))
                if rc == mqtt.MQTT_ERR_AGAIN:
                    break
                if client._read_ahead_exhausted and client._read_buffer_pending() == 0:
                    break
        batch_calls += 1
    elapsed = time.perf_counter() - started
    client._sock_close()
    server_thread.join(2.0)
    if server_errors:
        raise RuntimeError("TLS benchmark server failed: {}".format(server_errors[0]))
    return elapsed, (sock.recv_calls, batch_calls)


def _legacy_public_loop_read(iterations: int):
    client, delivered, sock = _new_burst_client(iterations)
    loop_calls = 0
    while delivered[0] < iterations:
        tracked_packets = len(client._out_messages) + len(client._in_messages)
        max_packets = max(tracked_packets, 1)
        for _ in range(max_packets):
            rc = client._packet_read()
            if rc > 0:
                raise RuntimeError("legacy public packet read failed: {}".format(rc))
            if rc == mqtt.MQTT_ERR_AGAIN:
                break
        loop_calls += 1
    return sock.recv_calls, loop_calls


def _candidate_public_loop_read(iterations: int):
    client, delivered, sock = _new_burst_client(iterations)
    loop_calls = 0
    while delivered[0] < iterations:
        tracked_packets = len(client._out_messages) + len(client._in_messages)
        max_packets = max(100, tracked_packets, 1)
        for _ in range(max_packets):
            rc = client._packet_read()
            if rc > 0:
                raise RuntimeError("candidate public packet read failed: {}".format(rc))
            if rc == mqtt.MQTT_ERR_AGAIN:
                break
        loop_calls += 1
    return sock.recv_calls, loop_calls


def _paired(label: str, legacy, candidate) -> None:
    for _ in range(WARMUPS):
        legacy(ITERATIONS)
        candidate(ITERATIONS)

    legacy_times = []
    candidate_times = []
    legacy_counts = candidate_counts = None
    for run in range(RUNS):
        order = (("legacy", legacy), ("candidate", candidate))
        if run % 2:
            order = tuple(reversed(order))
        for name, function in order:
            started = time.perf_counter()
            counts = function(ITERATIONS)
            elapsed = time.perf_counter() - started
            if name == "legacy":
                legacy_times.append(elapsed)
                legacy_counts = counts
            else:
                candidate_times.append(elapsed)
                candidate_counts = counts

    legacy_median = statistics.median(legacy_times)
    candidate_median = statistics.median(candidate_times)
    legacy_rate = ITERATIONS / legacy_median
    candidate_rate = ITERATIONS / candidate_median
    gain = (candidate_rate / legacy_rate - 1.0) * 100.0
    print(label)
    print("  legacy:    {:,.0f} msg/s [{:.6f}..{:.6f}] counts={}".format(
        legacy_rate, min(legacy_times), max(legacy_times), legacy_counts))
    print("  candidate: {:,.0f} msg/s [{:.6f}..{:.6f}] counts={}".format(
        candidate_rate, min(candidate_times), max(candidate_times), candidate_counts))
    print("  delta:     {:+.1f}%".format(gain))


def _paired_tls() -> None:
    for _ in range(WARMUPS):
        _tls_internal_batch(TLS_ITERATIONS, False)
        _tls_internal_batch(TLS_ITERATIONS, True)

    legacy_times = []
    candidate_times = []
    legacy_counts = candidate_counts = None
    for run in range(RUNS):
        order = (False, True) if run % 2 == 0 else (True, False)
        for candidate in order:
            elapsed, counts = _tls_internal_batch(TLS_ITERATIONS, candidate)
            if candidate:
                candidate_times.append(elapsed)
                candidate_counts = counts
            else:
                legacy_times.append(elapsed)
                legacy_counts = counts

    legacy_median = statistics.median(legacy_times)
    candidate_median = statistics.median(candidate_times)
    legacy_rate = TLS_ITERATIONS / legacy_median
    candidate_rate = TLS_ITERATIONS / candidate_median
    gain = (candidate_rate / legacy_rate - 1.0) * 100.0
    print("built-in TLS batch")
    print("  legacy:    {:,.0f} msg/s [{:.6f}..{:.6f}] counts={}".format(
        legacy_rate, min(legacy_times), max(legacy_times), legacy_counts))
    print("  candidate: {:,.0f} msg/s [{:.6f}..{:.6f}] counts={}".format(
        candidate_rate, min(candidate_times), max(candidate_times), candidate_counts))
    print("  delta:     {:+.1f}%".format(gain))


def _peak_memory(function, iterations: int) -> int:
    gc.collect()
    tracemalloc.start()
    function(iterations)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak


def main() -> int:
    _paired("built-in batch", _legacy_internal_batch, _candidate_internal_batch)
    _paired("public loop_read(100)", _legacy_public_loop_read, _candidate_public_loop_read)
    _paired_tls()
    legacy_peak = _peak_memory(_legacy_internal_batch, TLS_ITERATIONS)
    candidate_peak = _peak_memory(_candidate_internal_batch, TLS_ITERATIONS)
    print("tracemalloc peak, 3,000-message plain burst")
    print("  legacy:    {:,} B".format(legacy_peak))
    print("  candidate: {:,} B ({:+.1f}%)".format(
        candidate_peak, (candidate_peak / legacy_peak - 1.0) * 100.0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
