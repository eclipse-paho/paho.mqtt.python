#!/usr/bin/env python3
"""Local TLS handshake/session-resumption probe for audit project 25."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import socket
import ssl
import statistics
import sys
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SSL_DIR = ROOT / "tests" / "ssl"


def percentile(samples, fraction):
    ordered = sorted(samples)
    index = max(0, int(fraction * len(ordered) + 0.999999) - 1)
    return ordered[index]


def configure_version(context, version):
    selected = ssl.TLSVersion.TLSv1_2 if version == "1.2" else ssl.TLSVersion.TLSv1_3
    context.minimum_version = selected
    context.maximum_version = selected


def current_cpu_affinity():
    try:
        return sorted(os.sched_getaffinity(0))
    except AttributeError:
        return None


def server_worker(listener, contexts, transport, errors, reused):
    try:
        for context in contexts:
            raw, _address = listener.accept()
            with raw:
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                with context.wrap_socket(raw, server_side=True) as secured:
                    reused.append(bool(secured.session_reused))
                    if transport == "wss":
                        request = bytearray()
                        while b"\r\n\r\n" not in request:
                            chunk = secured.recv(4096)
                            if not chunk:
                                raise RuntimeError("truncated WebSocket handshake")
                            request.extend(chunk)
                        key = None
                        for line in bytes(request).split(b"\r\n"):
                            if line.lower().startswith(b"sec-websocket-key:"):
                                key = line.split(b":", 1)[1].strip()
                                break
                        if key is None:
                            raise RuntimeError("missing WebSocket key")
                        accept = base64.b64encode(hashlib.sha1(
                            key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                        ).digest())
                        secured.sendall(
                            b"HTTP/1.1 101 Switching Protocols\r\n"
                            b"Upgrade: websocket\r\n"
                            b"Connection: Upgrade\r\n"
                            b"Sec-WebSocket-Accept: " + accept + b"\r\n\r\n"
                        )
                    else:
                        if secured.recv(1) != b"x":
                            raise RuntimeError("client request mismatch")
                        secured.sendall(b"x")
    except Exception as error:  # pragma: no cover - surfaced in main thread
        errors.append(repr(error))
    finally:
        listener.close()


def make_server_context(version, certfile, keyfile):
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    configure_version(server_context, version)
    server_context.load_cert_chain(
        str(certfile),
        str(keyfile),
    )
    return server_context


def run_probe(version, mode, connections, certfile, keyfile, cafile, source=None,
              rotate_server_context=False, transport="raw",
              client_tcp_nodelay=False):
    server_context = make_server_context(version, certfile, keyfile)
    if rotate_server_context:
        server_contexts = [
            make_server_context(version, certfile, keyfile)
            for _ in range(connections)
        ]
    else:
        server_contexts = [server_context] * connections

    client_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    configure_version(client_context, version)
    client_context.load_verify_locations(str(cafile))
    client_context.check_hostname = True

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(16)
    port = listener.getsockname()[1]
    errors = []
    server_reused = []
    server = threading.Thread(
        target=server_worker,
        args=(listener, server_contexts, transport, errors, server_reused),
    )
    server.start()

    session = None
    mqtt_client = None
    if mode == "client":
        if source is None:
            raise ValueError("client mode requires --source")
        sys.path.insert(0, str(source.resolve() / "src"))
        import paho.mqtt.client as mqtt  # noqa: PLC0415
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        mqtt_client._ssl = True
        mqtt_client._ssl_context = client_context
        mqtt_client._tls_insecure = False
        mqtt_client._host = "localhost"
        mqtt_client._port = port
        if client_tcp_nodelay:
            create_socket_connection = mqtt_client._create_socket_connection

            def create_nodelay_socket():
                raw_socket = create_socket_connection()
                raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                return raw_socket

            mqtt_client._create_socket_connection = create_nodelay_socket
        if transport == "wss":
            mqtt_client._transport = "websockets"
    samples = []
    client_reused = []
    ticket_available = []
    for _ in range(connections):
        started = time.perf_counter()
        cpu_started = time.process_time()
        if transport == "wss":
            secured = mqtt_client._create_socket()
            tls_socket = secured._socket
        else:
            raw = socket.create_connection(("127.0.0.1", port), timeout=5)
            if client_tcp_nodelay:
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            wrap_options = {"server_hostname": "localhost"}
            if mode == "resume" and session is not None:
                wrap_options["session"] = session
            if mqtt_client is None:
                secured = client_context.wrap_socket(raw, **wrap_options)
            else:
                secured = mqtt_client._ssl_wrap_socket(raw)
            tls_socket = secured
        connected_at = time.perf_counter()
        try:
            if transport != "wss":
                secured.sendall(b"x")
                if secured.recv(1) != b"x":
                    raise RuntimeError("server response mismatch")
            current_session = tls_socket.session
            ticket_available.append(bool(getattr(current_session, "has_ticket", False)))
            client_reused.append(bool(tls_socket.session_reused))
            if mode == "resume":
                session = current_session
        finally:
            application_done_at = time.perf_counter()
            if mqtt_client is None:
                secured.close()
            else:
                mqtt_client._sock = secured
                mqtt_client._sock_close()
        closed_at = time.perf_counter()
        samples.append({
            "wall_ms": (closed_at - started) * 1000.0,
            "cpu_ms": (time.process_time() - cpu_started) * 1000.0,
            "connect_ms": (connected_at - started) * 1000.0,
            "application_ms": (application_done_at - connected_at) * 1000.0,
            "close_ms": (closed_at - application_done_at) * 1000.0,
        })

    server.join(5)
    if server.is_alive():
        raise RuntimeError("TLS server did not stop")
    if errors:
        raise RuntimeError("TLS server failed: {}".format(errors))

    # The first connection necessarily establishes the session. Reconnect
    # statistics exclude it in both full and resume modes.
    reconnects = samples[1:]
    wall = [sample["wall_ms"] for sample in reconnects]
    cpu = [sample["cpu_ms"] for sample in reconnects]
    connect = [sample["connect_ms"] for sample in reconnects]
    application = [sample["application_ms"] for sample in reconnects]
    close = [sample["close_ms"] for sample in reconnects]
    return {
        "python": sys.version.split()[0],
        "openssl": ssl.OPENSSL_VERSION,
        "cpu_affinity": current_cpu_affinity(),
        "source": str(source.resolve()) if source is not None else None,
        "tls": version,
        "mode": mode,
        "connections": connections,
        "reconnects": len(reconnects),
        "wall_median_ms": statistics.median(wall),
        "wall_p95_ms": percentile(wall, 0.95),
        "wall_min_ms": min(wall),
        "wall_max_ms": max(wall),
        "cpu_median_ms": statistics.median(cpu),
        "connect_median_ms": statistics.median(connect),
        "application_median_ms": statistics.median(application),
        "close_median_ms": statistics.median(close),
        "client_reused": sum(client_reused[1:]),
        "server_reused": sum(server_reused[1:]),
        "ticket_available": sum(ticket_available),
        "rotate_server_context": rotate_server_context,
        "transport": transport,
        "client_tcp_nodelay": client_tcp_nodelay,
        "server_session_stats": server_context.session_stats(),
    }


def aggregate_reports(reports, warmups):
    first = reports[0]
    wall = [report["wall_median_ms"] for report in reports]
    wall_p95 = [report["wall_p95_ms"] for report in reports]
    cpu = [report["cpu_median_ms"] for report in reports]
    connect = [report["connect_median_ms"] for report in reports]
    application = [report["application_median_ms"] for report in reports]
    close = [report["close_median_ms"] for report in reports]
    return {
        "python": first["python"],
        "openssl": first["openssl"],
        "cpu_affinity": first["cpu_affinity"],
        "source": first["source"],
        "tls": first["tls"],
        "mode": first["mode"],
        "transport": first["transport"],
        "connections_per_run": first["connections"],
        "reconnects_per_run": first["reconnects"],
        "runs": len(reports),
        "warmups": warmups,
        "wall_median_ms": statistics.median(wall),
        "wall_median_range_ms": [min(wall), max(wall)],
        "wall_p95_median_ms": statistics.median(wall_p95),
        "cpu_median_ms": statistics.median(cpu),
        "cpu_median_range_ms": [min(cpu), max(cpu)],
        "connect_median_ms": statistics.median(connect),
        "application_median_ms": statistics.median(application),
        "close_median_ms": statistics.median(close),
        "client_reused": sum(report["client_reused"] for report in reports),
        "server_reused": sum(report["server_reused"] for report in reports),
        "rotate_server_context": first["rotate_server_context"],
        "client_tcp_nodelay": first["client_tcp_nodelay"],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tls", choices=("1.2", "1.3"), required=True)
    parser.add_argument("--mode", choices=("full", "resume", "client"), required=True)
    parser.add_argument("--connections", type=int, default=21)
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--warmups", type=int, default=0)
    parser.add_argument("--certfile", type=Path, default=SSL_DIR / "server.crt")
    parser.add_argument("--keyfile", type=Path, default=SSL_DIR / "server.key")
    parser.add_argument("--cafile", type=Path, default=SSL_DIR / "all-ca.crt")
    parser.add_argument("--source", type=Path)
    parser.add_argument("--rotate-server-context", action="store_true")
    parser.add_argument("--transport", choices=("raw", "wss"), default="raw")
    parser.add_argument("--client-tcp-nodelay", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.connections < 2:
        parser.error("--connections must be at least 2")
    if args.runs < 1 or args.warmups < 0:
        parser.error("--runs must be positive and --warmups non-negative")
    if args.transport == "wss" and args.mode != "client":
        parser.error("--transport wss requires --mode client")

    def probe():
        return run_probe(
            args.tls,
            args.mode,
            args.connections,
            args.certfile,
            args.keyfile,
            args.cafile,
            args.source,
            args.rotate_server_context,
            args.transport,
            args.client_tcp_nodelay,
        )

    for _ in range(args.warmups):
        probe()
    reports = [probe() for _ in range(args.runs)]
    report = reports[0] if args.runs == 1 else aggregate_reports(
        reports, args.warmups
    )
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
