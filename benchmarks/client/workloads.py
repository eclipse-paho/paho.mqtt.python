"""Payload and topic fixtures for client benchmarks."""

from __future__ import annotations

import hashlib
import math
import random
import struct
from typing import Dict, List, Sequence, Tuple, Union

PayloadValue = Union[bytes, str]

# Header layout for integrity / latency payloads:
# magic(4) run_id(8) publisher_id(4) sequence(8) correlation(8) send_ns(8) = 40 bytes
HEADER_MAGIC = b"PMQ1"
HEADER_SIZE = 40


def encode_header(
    run_id: bytes,
    publisher_id: int,
    sequence: int,
    correlation: int,
    send_ns: int,
) -> bytes:
    if len(run_id) != 8:
        raise ValueError("run_id must be exactly 8 bytes")
    return struct.pack(
        "!4s8sIQQQ",
        HEADER_MAGIC,
        run_id,
        publisher_id & 0xFFFFFFFF,
        sequence & 0xFFFFFFFFFFFFFFFF,
        correlation & 0xFFFFFFFFFFFFFFFF,
        send_ns & 0xFFFFFFFFFFFFFFFF,
    )


def decode_header(payload: bytes) -> dict:
    if len(payload) < HEADER_SIZE:
        raise ValueError("payload too short for header")
    magic, run_id, publisher_id, sequence, correlation, send_ns = struct.unpack(
        "!4s8sIQQQ", payload[:HEADER_SIZE]
    )
    if magic != HEADER_MAGIC:
        raise ValueError("invalid payload magic")
    return {
        "run_id": run_id,
        "publisher_id": publisher_id,
        "sequence": sequence,
        "correlation": correlation,
        "send_ns": send_ns,
    }


def _pad_to(data: bytes, size: int, seed: int) -> bytes:
    if size < 0:
        raise ValueError("size must be >= 0")
    if size == 0:
        return b""
    if len(data) >= size:
        return data[:size]
    rng = random.Random(seed)
    pad = bytearray(size - len(data))
    for i in range(len(pad)):
        pad[i] = rng.randrange(256)
    return data + bytes(pad)


def make_telemetry_json(size: int, seed: int = 1) -> bytes:
    """Build a JSON-looking UTF-8 blob of exact `size` bytes."""
    base = (
        b'{"device":"d0001","ts":1710000000.123,"temp":21.5,'
        b'"humidity":48.2,"battery":93.0,"status":"ok","tags":["bench","iot"]}'
    )
    if size <= len(base):
        return _pad_to(base, size, seed)
    # Extend with a deterministic filler field.
    filler_needed = size - len(base) - len(b',"x":""')
    if filler_needed < 0:
        return _pad_to(base, size, seed)
    rng = random.Random(seed)
    alphabet = b"abcdefghijklmnopqrstuvwxyz0123456789"
    filler = bytes(alphabet[rng.randrange(len(alphabet))] for _ in range(filler_needed))
    blob = base[:-1] + b',"x":"' + filler + b'"}'
    return _pad_to(blob, size, seed)


PAYLOAD_SPECS = {
    "empty0": {"size": 0, "kind": "bytes"},
    "binary64": {"size": 64, "kind": "bytes"},
    "telemetry256": {"size": 256, "kind": "json"},
    "event1k": {"size": 1024, "kind": "json"},
    "record16k": {"size": 16 * 1024, "kind": "bytes"},
    "block64k": {"size": 64 * 1024, "kind": "bytes"},
    "block128k": {"size": 128 * 1024, "kind": "bytes"},
    "block256k": {"size": 256 * 1024, "kind": "bytes"},
    "block512k": {"size": 512 * 1024, "kind": "bytes"},
    "blob1m": {"size": 1024 * 1024, "kind": "bytes"},
    "blob8m": {"size": 8 * 1024 * 1024, "kind": "bytes"},
    "telemetry256_str": {"size": 256, "kind": "str"},
}


def build_payload(name: str, seed: int = 1) -> PayloadValue:
    spec = PAYLOAD_SPECS[name]
    size = spec["size"]
    kind = spec["kind"]
    if size == 0:
        return b""
    if kind == "json":
        data = make_telemetry_json(size, seed=seed)
    else:
        digest = hashlib.sha256(f"{name}:{seed}".encode()).digest()
        data = _pad_to(digest, size, seed)
    if kind == "str":
        return data.decode("utf-8", errors="replace")
    return data


def build_payload_corpus(
    name: str,
    count: int = 64,
    seed: int = 1,
    max_total_bytes: int = 64 * 1024 * 1024,
) -> List[PayloadValue]:
    """Pre-generate a circular corpus; clamp count to stay under memory cap."""
    spec = PAYLOAD_SPECS[name]
    size = max(spec["size"], 1)
    max_count = max(1, max_total_bytes // size)
    n = min(count, max_count)
    return [build_payload(name, seed=seed + i) for i in range(n)]


def wrap_with_header(body: bytes, header: bytes) -> bytes:
    if not body:
        return header
    if len(body) <= HEADER_SIZE:
        return header[: len(body)] if len(body) < HEADER_SIZE else header
    return header + body[HEADER_SIZE:]


def topic_for_device(run_id: str, site: int, device: int, metric: str) -> str:
    return (
        f"bench/{run_id}/org/acme/site/s{site:04d}/"
        f"device/d{device:04d}/telemetry/{metric}"
    )


METRICS = ("temperature", "humidity", "battery", "status")


def fleet_topics(run_id: str, devices: int = 1024, metrics: Sequence[str] = METRICS) -> List[str]:
    topics = []
    for device in range(devices):
        site = device // 64
        for metric in metrics:
            topics.append(topic_for_device(run_id, site, device, metric))
    return topics


def single_topic(run_id: str) -> str:
    return topic_for_device(run_id, 0, 0, "temperature")


def callback_match_topics(run_id: str, count: int) -> List[str]:
    """Exact topics used by sub_callback_matching (must match loadgen %i)."""
    if count < 1:
        raise ValueError("count must be >= 1")
    return [f"bench/{run_id}/org/acme/cb/{i}/data" for i in range(count)]


def callback_match_loadgen_topic(run_id: str) -> str:
    """emqtt-bench publish template; %i is the client sequence number."""
    return f"bench/{run_id}/org/acme/cb/%i/data"


def overlapping_match_filters(run_id: str, count: int) -> List[str]:
    """Distinct MQTT filters that all match callback_match_topics(...).

    Paho stores one callback per filter string, so filters must be unique.
    """
    if count < 1:
        raise ValueError("count must be >= 1")
    candidates = [
        f"bench/{run_id}/org/acme/#",
        f"bench/{run_id}/org/acme/cb/#",
        f"bench/{run_id}/org/acme/cb/+/data",
        f"bench/{run_id}/org/+/cb/+/data",
        f"bench/{run_id}/+/acme/cb/+/data",
        f"bench/{run_id}/#",
        f"bench/+/org/acme/cb/+/data",
        f"bench/+/org/acme/#",
    ]
    if count <= len(candidates):
        return candidates[:count]
    # Extend uniquely with explicit device indices under a wildcard parent.
    out = list(candidates)
    i = 0
    while len(out) < count:
        filt = f"bench/{run_id}/org/acme/cb/{i}/data"
        if filt not in out:
            out.append(filt)
        i += 1
        if i > count + 10:
            break
    return out[:count]


def wildcard_plus(run_id: str) -> str:
    return f"bench/{run_id}/org/acme/site/+/device/+/telemetry/+"


def wildcard_hash(run_id: str) -> str:
    return f"bench/{run_id}/org/acme/#"


def deep_topic(run_id: str, depth: int = 32) -> str:
    parts = [f"bench/{run_id}/deep"]
    for i in range(depth - 2):
        parts.append(f"l{i:02d}")
    parts.append("leaf")
    return "/".join(parts)


def long_topic(run_id: str, length: int) -> str:
    prefix = f"bench/{run_id}/long/"
    if length < len(prefix) + 1:
        raise ValueError("length too small")
    filler = "a" * (length - len(prefix))
    return prefix + filler


def unicode_topic(run_id: str) -> str:
    return f"bench/{run_id}/cap/bench/\u2603/temp"


def zipf_weights(n: int, alpha: float = 1.1) -> List[float]:
    raw = [1.0 / ((i + 1) ** alpha) for i in range(n)]
    total = sum(raw)
    return [w / total for w in raw]


def choose_index(weights: Sequence[float], rng: random.Random) -> int:
    r = rng.random()
    cumulative = 0.0
    for i, w in enumerate(weights):
        cumulative += w
        if r <= cumulative:
            return i
    return len(weights) - 1


def remaining_length_size(topic: str, qos: int, payload_len: int) -> int:
    """MQTT v3.1.1 PUBLISH remaining length (no properties)."""
    topic_len = len(topic.encode("utf-8"))
    packet_id_len = 2 if qos > 0 else 0
    return 2 + topic_len + packet_id_len + payload_len


def payload_len_for_remaining_length(topic: str, qos: int, target_rl: int) -> int:
    topic_len = len(topic.encode("utf-8"))
    packet_id_len = 2 if qos > 0 else 0
    fixed = 2 + topic_len + packet_id_len
    payload_len = target_rl - fixed
    if payload_len < 0:
        raise ValueError(f"topic too long for target remaining length {target_rl}")
    return payload_len


def rl_boundary_payloads(topic: str, qos: int = 0) -> Dict[str, int]:
    """Exact payload lengths around Remaining Length byte-width transitions."""
    return {
        "rl_126": payload_len_for_remaining_length(topic, qos, 126),
        "rl_127": payload_len_for_remaining_length(topic, qos, 127),
        "rl_128": payload_len_for_remaining_length(topic, qos, 128),
        "rl_16383": payload_len_for_remaining_length(topic, qos, 16383),
        "rl_16384": payload_len_for_remaining_length(topic, qos, 16384),
    }


def make_bytes_of_size(size: int, seed: int = 1) -> bytes:
    return _pad_to(b"", size, seed)
