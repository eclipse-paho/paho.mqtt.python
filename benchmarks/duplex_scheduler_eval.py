#!/usr/bin/env python3
"""Brokerless fairness evaluation for performance-audit project 19."""

from __future__ import annotations

import collections
import statistics
import struct
import time

import paho.mqtt.client as mqtt
import paho.mqtt.client as client_module

from fakes import NonBlockingRecvSocket, make_out_packet


RUNS = 15
WARMUPS = 2
OUTGOING = 10000
INCOMING = 100
TOPIC = b"duplex/in"


class _DuplexSocket(NonBlockingRecvSocket):
    def __init__(self, data):
        super().__init__(data)
        self.send_calls = 0

    def send(self, data):
        self.send_calls += 1
        return len(data)


def _incoming_wire():
    payload = b"x"
    remaining = 2 + len(TOPIC) + len(payload)
    packet = bytearray((mqtt.PUBLISH, remaining))
    packet.extend(struct.pack("!H", len(TOPIC)))
    packet.extend(TOPIC)
    packet.extend(payload)
    return bytes(packet) * INCOMING


def _new_client():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    sock = _DuplexSocket(_incoming_wire())
    client._sock = sock
    delivered = [0]
    client.on_message = lambda *args: delivered.__setitem__(0, delivered[0] + 1)
    wire = bytearray((mqtt.PUBACK, 2, 0, 1))
    client._out_packet = collections.deque(
        make_out_packet(mqtt.PUBACK, wire, mid=index + 1)
        for index in range(OUTGOING)
    )
    return client, sock, delivered


def _measure():
    client, sock, delivered = _new_client()
    original_select = client_module.select.select
    client_module.select.select = lambda *args: ([sock], [sock], [])
    try:
        started = time.perf_counter()
        rc = client._loop(0)
        elapsed = time.perf_counter() - started
    finally:
        client_module.select.select = original_select
    if rc != mqtt.MQTT_ERR_SUCCESS or delivered[0] != INCOMING:
        raise RuntimeError("duplex loop failed")
    return elapsed, sock.send_calls, len(client._out_packet)


def _measure_full_drain():
    client, sock, delivered = _new_client()
    original_select = client_module.select.select
    client_module.select.select = lambda *args: (
        ([sock] if sock._pos < len(sock._data) else []),
        [sock],
        [],
    )
    turns = 0
    try:
        started = time.perf_counter()
        while client._out_packet:
            rc = client._loop(0)
            turns += 1
        elapsed = time.perf_counter() - started
    finally:
        client_module.select.select = original_select
    if rc != mqtt.MQTT_ERR_SUCCESS or delivered[0] != INCOMING:
        raise RuntimeError("duplex full drain failed")
    return elapsed, turns


def main():
    for _ in range(WARMUPS):
        _measure()
    samples = [_measure() for _ in range(RUNS)]
    elapsed = [sample[0] for sample in samples]
    print("100 inbound + 10000 queued outbound")
    print("  loop median: {:.3f} ms [{:.3f}..{:.3f}]".format(
        statistics.median(elapsed) * 1000,
        min(elapsed) * 1000,
        max(elapsed) * 1000,
    ))
    print("  sends/turn: {}; remaining: {}".format(
        statistics.median(sample[1] for sample in samples),
        statistics.median(sample[2] for sample in samples),
    ))
    full = [_measure_full_drain() for _ in range(RUNS)]
    print("  full drain: {:.3f} ms; turns: {}".format(
        statistics.median(sample[0] for sample in full) * 1000,
        statistics.median(sample[1] for sample in full),
    ))


if __name__ == "__main__":
    main()
