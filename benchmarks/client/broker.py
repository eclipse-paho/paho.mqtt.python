"""Mosquitto broker lifecycle, certificates and health checks."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import time
from pathlib import Path
from typing import Optional, Tuple

CLIENT_DIR = Path(__file__).resolve().parent
COMPOSE_FILE = CLIENT_DIR / "docker-compose.yml"
MOSQUITTO_CONF = CLIENT_DIR / "mosquitto" / "mosquitto.conf"
CERT_DIR = CLIENT_DIR / "certs"

# Pin by tag; digest is recorded at runtime after pull/inspect.
MOSQUITTO_IMAGE = os.environ.get(
    "PAHO_BENCH_MOSQUITTO_IMAGE",
    "eclipse-mosquitto:2.0.20@sha256:21421af7b32bf9ce508e9090c8eb13bb81f410ca778dc205506180a6f862d0eb",
)
EMQTT_BENCH_IMAGE = os.environ.get(
    "PAHO_BENCH_EMQTT_IMAGE",
    "emqx/emqtt-bench:latest@sha256:ae7f2d56cd49b14824c835140c808b093c5e3f2defb3a29b34b17560feb456cd",
)

DEFAULT_HOST = "127.0.0.1"
# Dedicated ports so the harness does not collide with a system Mosquitto on 1883/8883.
DEFAULT_PORT = 11883
DEFAULT_TLS_PORT = 11884


def _run(cmd, *, check=True, capture=True, env=None):
    return subprocess.run(
        cmd,
        check=check,
        capture_output=capture,
        text=True,
        env=env,
    )


def config_hash() -> str:
    data = MOSQUITTO_CONF.read_bytes() if MOSQUITTO_CONF.exists() else b""
    return hashlib.sha256(data).hexdigest()


def image_digest(image: str) -> Optional[str]:
    try:
        proc = _run(["docker", "image", "inspect", "--format", "{{index .RepoDigests 0}}", image], check=False)
        if proc.returncode != 0:
            return None
        digest = (proc.stdout or "").strip()
        return digest or None
    except FileNotFoundError:
        return None


def ensure_certs(force: bool = False) -> dict:
    """Generate a dedicated benchmark CA + server cert with SANs."""
    CERT_DIR.mkdir(parents=True, exist_ok=True)
    ca_key = CERT_DIR / "ca.key"
    ca_crt = CERT_DIR / "ca.crt"
    server_key = CERT_DIR / "server.key"
    server_crt = CERT_DIR / "server.crt"
    openssl_cfg = CERT_DIR / "openssl.cnf"

    if server_crt.exists() and ca_crt.exists() and server_key.exists() and ca_key.exists() and not force:
        os.chmod(server_key, 0o644)
        return {
            "ca_crt": str(ca_crt),
            "server_crt": str(server_crt),
            "server_key": str(server_key),
            "fingerprint": _fingerprint(server_crt),
        }

    openssl_cfg.write_text(
        """
[req]
distinguished_name = req_distinguished_name
x509_extensions = v3_req
prompt = no

[req_distinguished_name]
CN = paho-bench-ca

[v3_req]
basicConstraints = CA:TRUE
keyUsage = keyCertSign, cRLSign

[server_req]
distinguished_name = server_dn
req_extensions = server_ext
prompt = no

[server_dn]
CN = localhost

[server_ext]
subjectAltName = @alt_names
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth

[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
""".strip()
        + "\n"
    )

    _run(["openssl", "genrsa", "-out", str(ca_key), "2048"])
    _run(
        [
            "openssl",
            "req",
            "-x509",
            "-new",
            "-nodes",
            "-key",
            str(ca_key),
            "-sha256",
            "-days",
            "3650",
            "-out",
            str(ca_crt),
            "-config",
            str(openssl_cfg),
            "-extensions",
            "v3_req",
        ]
    )
    _run(["openssl", "genrsa", "-out", str(server_key), "2048"])
    # The container runs as uid 1883 and must be able to read the throwaway key.
    os.chmod(server_key, 0o644)
    csr = CERT_DIR / "server.csr"
    _run(
        [
            "openssl",
            "req",
            "-new",
            "-key",
            str(server_key),
            "-out",
            str(csr),
            "-config",
            str(openssl_cfg),
            "-section",
            "server_req",
        ]
    )
    _run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(ca_crt),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-out",
            str(server_crt),
            "-days",
            "825",
            "-sha256",
            "-extfile",
            str(openssl_cfg),
            "-extensions",
            "server_ext",
        ]
    )
    return {
        "ca_crt": str(ca_crt),
        "server_crt": str(server_crt),
        "server_key": str(server_key),
        "fingerprint": _fingerprint(server_crt),
    }


def _fingerprint(cert_path: Path) -> str:
    proc = _run(["openssl", "x509", "-in", str(cert_path), "-noout", "-fingerprint", "-sha256"])
    return (proc.stdout or "").strip()


def compose_cmd(*args: str) -> list:
    return ["docker", "compose", "-f", str(COMPOSE_FILE), *args]


_BROKER_CONTAINER_CACHE: Optional[str] = None


def broker_container_name() -> str:
    """Return the actual compose container name for the mosquitto service."""
    global _BROKER_CONTAINER_CACHE
    if _BROKER_CONTAINER_CACHE is not None:
        return _BROKER_CONTAINER_CACHE
    for args in (("ps", "-a", "--format", "{{.Name}}", "mosquitto"), ("ps", "-a", "-q", "mosquitto")):
        try:
            proc = _run(compose_cmd(*args), check=False)
        except FileNotFoundError:
            break
        lines = [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]
        if proc.returncode == 0 and lines:
            _BROKER_CONTAINER_CACHE = lines[0]
            return lines[0]
    return "mosquitto"


def _container_state(name: str) -> Optional[str]:
    try:
        proc = _run(["docker", "inspect", "--format", "{{.State.Status}}", name], check=False)
    except FileNotFoundError:
        return None
    if proc.returncode != 0:
        return None
    return (proc.stdout or "").strip() or None


def broker_up(wait: bool = True, timeout_s: float = 30.0) -> dict:
    ensure_certs()
    _run(compose_cmd("up", "-d", "mosquitto"))
    container = broker_container_name()
    deadline = time.time() + 10.0
    state = _container_state(container)
    while state != "running" and time.time() < deadline:
        time.sleep(0.5)
        state = _container_state(container)
    if state == "running":
        time.sleep(1.5)
        state = _container_state(container)
    if state != "running":
        logs = _run(["docker", "logs", "--tail", "5", container], check=False)
        raise RuntimeError(
            "managed mosquitto container {!r} is {}; another broker may hold ports {}/{}. "
            "Last logs: {!r}".format(
                container,
                state or "absent",
                DEFAULT_PORT,
                DEFAULT_TLS_PORT,
                (logs.stdout or logs.stderr or "").strip(),
            )
        )
    meta = {
        "managed_broker": True,
        "image": MOSQUITTO_IMAGE,
        "image_digest": image_digest(MOSQUITTO_IMAGE),
        "config_hash": config_hash(),
        "host": DEFAULT_HOST,
        "port": DEFAULT_PORT,
        "tls_port": DEFAULT_TLS_PORT,
        "container_name": container,
        "certs": ensure_certs(),
    }
    if wait:
        wait_for_broker(DEFAULT_HOST, DEFAULT_PORT, timeout_s=timeout_s)
        wait_for_broker(DEFAULT_HOST, DEFAULT_TLS_PORT, timeout_s=timeout_s, tls=True, ca_certs=meta["certs"]["ca_crt"])
    return meta


def broker_down() -> None:
    _run(compose_cmd("down", "--remove-orphans"), check=False)


def wait_for_broker(
    host: str,
    port: int,
    *,
    timeout_s: float = 30.0,
    tls: bool = False,
    ca_certs: Optional[str] = None,
) -> None:
    """Broker ready means MQTT CONNACK success, not merely TCP accept."""
    deadline = time.time() + timeout_s
    last_err = None
    while time.time() < deadline:
        try:
            _mqtt_ping(host, port, tls=tls, ca_certs=ca_certs)
            return
        except Exception as exc:  # noqa: BLE001 - collect and retry until timeout
            last_err = exc
            time.sleep(0.25)
    raise TimeoutError(f"broker not ready at {host}:{port}: {last_err}")


def _mqtt_ping(host: str, port: int, *, tls: bool = False, ca_certs: Optional[str] = None) -> None:
    """Minimal MQTT CONNECT/CONNACK using stdlib sockets (no paho import in orchestrator)."""
    sock = socket.create_connection((host, port), timeout=3.0)
    try:
        if tls:
            import ssl

            ctx = ssl.create_default_context(cafile=ca_certs) if ca_certs else ssl.create_default_context()
            ctx.check_hostname = True
            sock = ctx.wrap_socket(sock, server_hostname=host)
        # MQTT 3.1.1 CONNECT with client_id "benchping", clean session, keepalive 10
        client_id = b"benchping"
        # Variable header + payload
        proto_name = b"MQTT"
        vh = struct_pack_string(proto_name) + bytes([0x04, 0x02, 0x00, 0x0A])
        payload = struct_pack_string(client_id)
        remaining = vh + payload
        packet = bytes([0x10]) + encode_remaining_length(len(remaining)) + remaining
        sock.sendall(packet)
        # Expect CONNACK: 20 02 00 00
        data = _recv_exact(sock, 4)
        if data[0] != 0x20 or data[3] != 0x00:
            raise RuntimeError(f"unexpected CONNACK: {data!r}")
    finally:
        sock.close()


def struct_pack_string(value: bytes) -> bytes:
    return len(value).to_bytes(2, "big") + value


def encode_remaining_length(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value % 128
        value //= 128
        if value > 0:
            byte |= 0x80
        out.append(byte)
        if value == 0:
            break
    return bytes(out)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("socket closed while reading CONNACK")
        buf.extend(chunk)
    return bytes(buf)


def parse_broker_endpoint(value: str) -> Tuple[str, int]:
    if ":" in value:
        host, port_s = value.rsplit(":", 1)
        return host, int(port_s)
    return value, DEFAULT_PORT
