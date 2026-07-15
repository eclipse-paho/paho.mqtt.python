#!/usr/bin/env python3
"""Paired local-socket evaluation for segmented large PUBLISH packets."""

from __future__ import annotations

import socket
import statistics
import threading
import time

import paho.mqtt.client as mqtt


RUNS = 7
WARMUPS = 2


class _CountingSocket:
    def __init__(self, sock):
        self._sock = sock
        self.calls = 0
        self.first_send = None

    def send(self, data):
        if self.first_send is None:
            self.first_send = time.perf_counter()
        self.calls += 1
        return self._sock.send(data)

    def close(self):
        self._sock.close()


def _socket_pair(transport):
    if transport == "unix":
        return socket.socketpair()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    writer = socket.create_connection(listener.getsockname())
    reader, _ = listener.accept()
    listener.close()
    return writer, reader


def _measure(payload, transport, segmented):
    writer, reader = _socket_pair(transport)
    proxy = _CountingSocket(writer)
    received = [0]

    def drain():
        while True:
            data = reader.recv(262144)
            if not data:
                return
            received[0] += len(data)

    thread = threading.Thread(target=drain)
    thread.start()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client._sock = proxy
    client._thread = threading.Thread(target=lambda: None)
    if not segmented:
        client._transport = "websockets"  # Force the current contiguous builder only.

    started = time.perf_counter()
    rc = client._send_publish(1, b"bench/large", payload, qos=1)
    client._transport = "tcp"
    if rc == mqtt.MQTT_ERR_SUCCESS:
        rc = client._packet_write()
    writer.shutdown(socket.SHUT_WR)
    thread.join()
    elapsed = time.perf_counter() - started
    first_send = proxy.first_send - started
    writer.close()
    reader.close()
    if rc != mqtt.MQTT_ERR_SUCCESS or received[0] < len(payload):
        raise RuntimeError("socket payload did not drain")
    return elapsed, first_send, proxy.calls


def main():
    for transport in ("unix", "tcp"):
        for size in (128, 1024 * 1024, 64 * 1024 * 1024):
            payload = b"x" * size
            for _ in range(WARMUPS):
                _measure(payload, transport, False)
                _measure(payload, transport, True)
            legacy = []
            candidate = []
            for run in range(RUNS):
                order = ((legacy, False), (candidate, True))
                if run % 2:
                    order = tuple(reversed(order))
                for samples, segmented in order:
                    samples.append(_measure(payload, transport, segmented))
            legacy_total = statistics.median(sample[0] for sample in legacy)
            candidate_total = statistics.median(sample[0] for sample in candidate)
            print("{} {} bytes".format(transport, size))
            print("  total: {:.3f} -> {:.3f} ms ({:+.1f}%)".format(
                legacy_total * 1000,
                candidate_total * 1000,
                (legacy_total / candidate_total - 1.0) * 100.0,
            ))
            print("  first send: {:.3f} -> {:.3f} ms; calls {} -> {}".format(
                statistics.median(sample[1] for sample in legacy) * 1000,
                statistics.median(sample[1] for sample in candidate) * 1000,
                statistics.median(sample[2] for sample in legacy),
                statistics.median(sample[2] for sample in candidate),
            ))


if __name__ == "__main__":
    main()
