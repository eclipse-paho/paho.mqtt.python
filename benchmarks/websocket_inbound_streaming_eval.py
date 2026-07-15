#!/usr/bin/env python3
"""Brokerless evaluation for performance-audit project 21."""

from __future__ import annotations

import argparse
import gc
import json
import socket
import ssl
import statistics
import struct
import sys
import time
import tracemalloc
import threading
from pathlib import Path


def _remaining_length(value):
    result = bytearray()
    while True:
        byte = value % 128
        value //= 128
        if value:
            byte |= 0x80
        result.append(byte)
        if not value:
            return bytes(result)


def _mqtt_publish(payload):
    topic = b"audit/ws"
    body = struct.pack("!H", len(topic)) + topic + payload
    return b"\x30" + _remaining_length(len(body)) + body


def _frame(opcode, payload, *, final=True):
    first = (0x80 if final else 0) | opcode
    length = len(payload)
    if length < 126:
        header = bytes((first, length))
    elif length < 65536:
        header = bytes((first, 126)) + struct.pack("!H", length)
    else:
        header = bytes((first, 127)) + struct.pack("!Q", length)
    return header + payload


class _RawSocket:
    def __init__(self, data, chunk_size):
        self.data = bytes(data)
        self.position = 0
        self.chunk_size = chunk_size
        self.recv_calls = 0
        self.sent = bytearray()

    def recv(self, size):
        self.recv_calls += 1
        if self.position == len(self.data):
            raise BlockingIOError()
        count = min(size, self.chunk_size, len(self.data) - self.position)
        result = self.data[self.position:self.position + count]
        self.position += count
        return result

    def send(self, data):
        self.sent.extend(bytes(data))
        return len(data)

    def pending(self):
        return len(self.data) - self.position

    def close(self):
        return None

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


class _CountingSocket:
    def __init__(self, sock):
        self.sock = sock
        self.recv_calls = 0

    def recv(self, size):
        self.recv_calls += 1
        return self.sock.recv(size)

    def send(self, data):
        return self.sock.send(data)

    def pending(self):
        return self.sock.pending()

    def close(self):
        return self.sock.close()

    def fileno(self):
        return self.sock.fileno()

    def setblocking(self, flag):
        return self.sock.setblocking(flag)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--runs", type=int, default=7)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--scenario", action="append", dest="selected_scenarios")
    args = parser.parse_args()

    source = Path(args.source).resolve()
    sys.path.insert(0, str(source / "src"))
    from paho.mqtt import client as mqtt  # noqa: PLC0415
    from paho.mqtt.enums import CallbackAPIVersion  # noqa: PLC0415

    def wrapper(raw, is_ssl=False):
        result = mqtt._WebsocketWrapper.__new__(mqtt._WebsocketWrapper)
        result.connected = True
        result._ssl = is_ssl
        result._socket = raw
        result._sendbuffer = bytearray()
        result._sendbuffer_head = 0
        result._readbuffer = bytearray()
        result._requested_size = 0
        result._payload_head = 0
        result._readbuffer_head = 0
        # Candidate-only state is initialized by a private helper when present.
        initialize = getattr(result, "_reset_inbound", None)
        if initialize is not None:
            initialize()
        return result

    small_packet = _mqtt_publish(b"x" * 32)
    large_packet = _mqtt_publish(b"x" * (64 * 1024))

    def one_per_frame(count):
        return b"".join(_frame(0x2, small_packet) for _ in range(count))

    def hundred_per_frame(count):
        group = _frame(0x2, small_packet * 100)
        return group * (count // 100)

    def fragmented(count):
        frames = []
        for _ in range(count):
            offsets = [index * len(small_packet) // 8 for index in range(9)]
            for index in range(8):
                frames.append(_frame(
                    0x2 if index == 0 else 0x0,
                    small_packet[offsets[index]:offsets[index + 1]],
                    final=index == 7,
                ))
        return b"".join(frames)

    scenarios = (
        ("one_packet_per_frame", 1000, one_per_frame(1000), len(small_packet), 65536),
        ("hundred_packets_per_frame", 1000, hundred_per_frame(1000), len(small_packet), 65536),
        ("eight_fragments_per_packet", 1000, fragmented(1000), len(small_packet), 65536),
        ("one_packet_per_frame_chunk7", 1000, one_per_frame(1000), len(small_packet), 7),
        ("large_packet_per_frame", 100, _frame(0x2, large_packet) * 100, len(large_packet), 65536),
    )

    def run_once(message_count, stream, packet_size, chunk_size, memory=False):
        raw = _RawSocket(stream, chunk_size)
        ws = wrapper(raw)
        mqttc = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv311,
            transport="websockets",
        )
        delivered = [0]
        mqttc.on_message = lambda *unused: delivered.__setitem__(0, delivered[0] + 1)
        mqttc._sock = ws

        wrapper_peak = [0]
        compact = getattr(ws, "_compact_inbound", None)
        if memory and compact is not None:
            def tracked_compact():
                wrapper_peak[0] = max(
                    wrapper_peak[0],
                    len(ws._readbuffer) + len(ws._decoded_buffer),
                )
                compact()
                wrapper_peak[0] = max(
                    wrapper_peak[0],
                    len(ws._readbuffer) + len(ws._decoded_buffer),
                )

            ws._compact_inbound = tracked_compact

        gc.collect()
        if memory:
            tracemalloc.start()
            tracemalloc.reset_peak()
        gc_enabled = gc.isenabled()
        gc.disable()
        started_cpu = time.process_time()
        started = time.perf_counter()
        batch_calls = 0
        try:
            while delivered[0] < message_count:
                rc = mqttc._loop_read_batch(100)
                if rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError("WebSocket batch failed: {}".format(rc))
                batch_calls += 1
                if batch_calls > len(stream) + message_count:
                    raise RuntimeError("WebSocket batch made no progress")
            elapsed = time.perf_counter() - started
            cpu = time.process_time() - started_cpu
        finally:
            if gc_enabled:
                gc.enable()
        peak = 0
        if memory:
            _current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
        if raw.position != len(stream):
            raise RuntimeError("not all WebSocket input was consumed")
        return {
            "elapsed_s": elapsed,
            "cpu_s": cpu,
            "recv_calls": raw.recv_calls,
            "batch_calls": batch_calls,
            "peak_bytes": peak,
            "wrapper_peak_buffered_bytes": wrapper_peak[0] or None,
            "packet_size": packet_size,
        }

    output = {}
    for name, count, stream, packet_size, chunk_size in scenarios:
        if args.selected_scenarios and name not in args.selected_scenarios:
            continue
        for _ in range(args.warmups):
            run_once(count, stream, packet_size, chunk_size)
        samples = [
            run_once(count, stream, packet_size, chunk_size)
            for _ in range(args.runs)
        ]
        memory = run_once(count, stream, packet_size, chunk_size, memory=True)
        elapsed = [sample["elapsed_s"] for sample in samples]
        cpu = [sample["cpu_s"] for sample in samples]
        median = statistics.median(elapsed)
        output[name] = {
            "messages": count,
            "packet_size": packet_size,
            "raw_chunk_size": chunk_size,
            "msg_per_s": count / median,
            "elapsed_median_ms": median * 1000.0,
            "elapsed_min_ms": min(elapsed) * 1000.0,
            "elapsed_max_ms": max(elapsed) * 1000.0,
            "cpu_median_ms": statistics.median(cpu) * 1000.0,
            "recv_calls": samples[0]["recv_calls"],
            "batch_calls": samples[0]["batch_calls"],
            "tracemalloc_peak_bytes": memory["peak_bytes"],
            "wrapper_peak_buffered_bytes": memory["wrapper_peak_buffered_bytes"],
        }

    if not args.selected_scenarios or "wss_one_packet_per_frame" in args.selected_scenarios:
        tls_stream = one_per_frame(1000)
        ssl_dir = Path(__file__).resolve().parents[1] / "tests" / "ssl"
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(
            str(ssl_dir / "server.crt"),
            str(ssl_dir / "server.key"),
        )
        client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        client_context.check_hostname = False
        client_context.verify_mode = ssl.CERT_NONE

        def run_tls_once():
            server_raw, client_raw = socket.socketpair()
            server_errors = []

            def serve():
                try:
                    with server_context.wrap_socket(server_raw, server_side=True) as server:
                        server.sendall(tls_stream)
                except Exception as error:  # pragma: no cover - benchmark diagnostic
                    server_errors.append(error)

            server_thread = threading.Thread(target=serve, name="plan21-wss-server")
            server_thread.start()
            tls_socket = client_context.wrap_socket(client_raw, server_hostname="localhost")
            counted = _CountingSocket(tls_socket)
            ws = wrapper(counted, is_ssl=True)
            mqttc = mqtt.Client(
                callback_api_version=CallbackAPIVersion.VERSION2,
                protocol=mqtt.MQTTv311,
                transport="websockets",
            )
            delivered = [0]
            mqttc.on_message = lambda *unused: delivered.__setitem__(0, delivered[0] + 1)
            mqttc._sock = ws
            batch_calls = 0
            started_cpu = time.process_time()
            started = time.perf_counter()
            while delivered[0] < 1000:
                rc = mqttc._loop_read_batch(100)
                if rc != mqtt.MQTT_ERR_SUCCESS:
                    raise RuntimeError("WSS batch failed: {}".format(rc))
                batch_calls += 1
            elapsed = time.perf_counter() - started
            cpu = time.process_time() - started_cpu
            mqttc._sock_close()
            server_thread.join(2.0)
            if server_errors:
                raise RuntimeError("WSS server failed: {}".format(server_errors[0]))
            return elapsed, cpu, counted.recv_calls, batch_calls

        for _ in range(args.warmups):
            run_tls_once()
        tls_samples = [run_tls_once() for _ in range(args.runs)]
        tls_elapsed = [sample[0] for sample in tls_samples]
        output["wss_one_packet_per_frame"] = {
            "messages": 1000,
            "packet_size": len(small_packet),
            "msg_per_s": 1000 / statistics.median(tls_elapsed),
            "elapsed_median_ms": statistics.median(tls_elapsed) * 1000.0,
            "elapsed_min_ms": min(tls_elapsed) * 1000.0,
            "elapsed_max_ms": max(tls_elapsed) * 1000.0,
            "cpu_median_ms": statistics.median(sample[1] for sample in tls_samples) * 1000.0,
            "recv_calls": tls_samples[0][2],
            "batch_calls": tls_samples[0][3],
            "tracemalloc_peak_bytes": None,
        }

    print(json.dumps({
        "source": str(source),
        "runs": args.runs,
        "warmups": args.warmups,
        "scenarios": output,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
