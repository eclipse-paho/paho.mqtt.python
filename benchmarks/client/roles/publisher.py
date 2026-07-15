"""
Publisher worker process.

Usage:
  python roles/publisher.py --config /path/config.json
"""

from __future__ import annotations

import argparse
import gc
import os
import sys
import threading
import time
from pathlib import Path

ROLE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = ROLE_DIR.parent
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

from control import barrier_client_wait, configure_source_root, touch, write_json  # noqa: E402
from workloads import (  # noqa: E402
    HEADER_SIZE,
    build_payload,
    build_payload_corpus,
    encode_header,
    make_bytes_of_size,
    rl_boundary_payloads,
    single_topic,
    wrap_with_header,
)


def _protocol_const(mqtt, name: str):
    return getattr(mqtt, name)


class _CountingSocket:
    def __init__(self, sock):
        self._sock = sock
        self.send_calls = 0
        self.sendmsg_calls = 0
        self.sendmsg_iovecs = 0

    def send(self, data):
        self.send_calls += 1
        return self._sock.send(data)

    def sendmsg(self, buffers, *args):
        self.sendmsg_calls += 1
        self.sendmsg_iovecs += len(buffers)
        return self._sock.sendmsg(buffers, *args)

    def __getattr__(self, name):
        return getattr(self._sock, name)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)

    import json

    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    paho_file = configure_source_root(cfg["source_root"])
    import paho.mqtt.client as mqtt
    from paho.mqtt.properties import Properties
    from paho.mqtt.packettypes import PacketTypes

    run_id = cfg["run_id"].encode("ascii")
    if len(run_id) != 8:
        raise SystemExit("run_id must be 8 ascii chars")

    topic = cfg.get("topic") or single_topic(cfg["run_id"])
    qos = int(cfg.get("qos_publish", 0))
    duration_s = float(cfg.get("duration_s", 3.0))
    warmup_s = float(cfg.get("warmup_s", 1.0))
    drain_s = float(cfg.get("drain_s", 2.0))
    outstanding = int(cfg.get("outstanding", 64))
    inflight = int(cfg.get("inflight", 20))
    max_queued = int(cfg.get("max_queued", 200))
    cadence = cfg.get("cadence", "capacity")
    load_fraction = float(cfg.get("load_fraction", 0.75))
    target_rate = cfg.get("target_rate")  # msgs/s for open-loop
    payload_name = cfg.get("payload", "telemetry256")

    # Build payload body.
    if payload_name.startswith("rl_"):
        sizes = rl_boundary_payloads(topic, qos=qos)
        body = make_bytes_of_size(sizes[payload_name], seed=1)
    else:
        raw = build_payload(payload_name, seed=1)
        body = raw.encode("utf-8") if isinstance(raw, str) else raw

    corpus = []
    if payload_name in ("telemetry256", "event1k", "binary64") and not payload_name.startswith("rl_"):
        corpus = build_payload_corpus(payload_name, count=64, seed=7)
        corpus = [c.encode("utf-8") if isinstance(c, str) else c for c in corpus]

    state = {
        "connected": threading.Event(),
        "publish_calls": 0,
        "publish_accepted": 0,
        "publish_rejected": 0,
        "protocol_completed": 0,
        "protocol_failed": 0,
        "socket_completed_qos0": 0,
        "completed_in_window": 0,
        "completed_during_drain": 0,
        "latencies_ns": [],
        "scheduler_lags_ns": [],
        "lock": threading.Lock(),
        "inflight_local": 0,
        "phase": "init",
        "mid_send_ns": {},
    }

    def on_connect(client, userdata, flags, reason_code, properties=None):
        rc = int(getattr(reason_code, "value", reason_code))
        if rc == 0:
            state["connected"].set()

    def on_publish(client, userdata, mid, reason_code=None, properties=None):
        now = time.perf_counter_ns()
        with state["lock"]:
            failed = False
            if reason_code is not None:
                rc = int(getattr(reason_code, "value", reason_code))
                if rc >= 128:
                    failed = True
            # Always release the mid slot; a failure still ends the outstanding operation.
            send_ns = state["mid_send_ns"].pop(mid, None)
            if failed:
                state["protocol_failed"] += 1
            else:
                if qos == 0:
                    state["socket_completed_qos0"] += 1
                else:
                    state["protocol_completed"] += 1
                if send_ns is not None:
                    state["latencies_ns"].append(now - send_ns)
                if state["phase"] == "measure":
                    state["completed_in_window"] += 1
                elif state["phase"] == "drain":
                    state["completed_during_drain"] += 1
            state["inflight_local"] = max(0, state["inflight_local"] - 1)

    protocol = _protocol_const(mqtt, cfg.get("protocol", "MQTTv311"))
    client_kwargs = {
        "callback_api_version": mqtt.CallbackAPIVersion.VERSION2,
        "client_id": cfg.get("client_id", f"pub-{cfg['run_id']}"),
        "protocol": protocol,
    }
    if cfg.get("protocol", "MQTTv311") != "MQTTv5":
        client_kwargs["clean_session"] = not bool(cfg.get("session_persistent", False))

    client = mqtt.Client(**client_kwargs)
    client.max_inflight_messages = inflight
    client.max_queued_messages = max_queued
    client.on_connect = on_connect
    client.on_publish = on_publish
    counted_socket = None
    if cfg.get("count_socket_writes"):
        def on_socket_open(opened_client, userdata, sock):
            nonlocal counted_socket
            counted_socket = _CountingSocket(sock)
            opened_client._sock = counted_socket

        client.on_socket_open = on_socket_open

    if cfg.get("tls"):
        client.tls_set(ca_certs=cfg["ca_certs"])

    host = cfg["host"]
    port = int(cfg["port"])
    client.connect(host, port, keepalive=int(cfg.get("keepalive", 60)))
    client.loop_start()
    if not state["connected"].wait(timeout=30):
        write_json(cfg["result_path"], {"ok": False, "error": "connect_timeout", "paho_file": paho_file})
        client.loop_stop()
        return 1

    touch(cfg["ready_path"], {"role": "publisher", "paho_file": paho_file, "pid": os.getpid()})

    # Wait for T0 barrier.
    barrier_client_wait(cfg["barrier_path"], "T0", timeout_s=float(cfg.get("barrier_timeout_s", 120)))

    open_loop_rate = None
    if cadence in ("steady50", "loaded75", "loaded90", "periodic10") or cfg.get("load_fraction"):
        if target_rate:
            open_loop_rate = float(target_rate)
        else:
            # Fallback rate if calibration missing.
            open_loop_rate = 1000.0 * load_fraction
        if cadence == "steady50":
            open_loop_rate = (target_rate or 2000.0) * 0.50
        elif cadence == "periodic10":
            open_loop_rate = 10.0

    gc.collect()
    gc_start = gc.get_count()
    state["phase"] = "warmup"
    warmup_end = time.perf_counter() + warmup_s
    # Warm up at the measure cadence: a capacity warmup before an open-loop
    # measure floods broker/subscriber queues and corrupts integrity windows.
    # Warmup sequences live in a disjoint range so late deliveries can never
    # collide with measure-window sequence numbers.
    _run_publish_loop(
        client,
        state,
        topic=topic,
        qos=qos,
        body=body,
        corpus=corpus,
        run_id=run_id,
        outstanding=outstanding,
        cadence=cadence,
        until=warmup_end,
        target_rate=open_loop_rate,
        properties_builder=_properties_builder(cfg, mqtt, Properties, PacketTypes),
        force_header=bool(cfg.get("force_header", False)),
        sequence_start=1 << 40,
    )

    # Reset window counters after warmup and wait for outstanding to drain
    # so measure-phase sequence numbers stay contiguous for integrity checks.
    drain_warmup = time.perf_counter() + min(drain_s, 5.0)
    while time.perf_counter() < drain_warmup:
        with state["lock"]:
            if state["inflight_local"] == 0 and not state["mid_send_ns"]:
                break
        time.sleep(0.01)

    with state["lock"]:
        state["completed_in_window"] = 0
        state["completed_during_drain"] = 0
        state["latencies_ns"].clear()
        state["scheduler_lags_ns"].clear()
        state["publish_calls"] = 0
        state["publish_accepted"] = 0
        state["publish_rejected"] = 0
        state["protocol_completed"] = 0
        state["protocol_failed"] = 0
        state["socket_completed_qos0"] = 0
        state["mid_send_ns"].clear()
        state["inflight_local"] = 0
    if counted_socket is not None:
        counted_socket.send_calls = 0
        counted_socket.sendmsg_calls = 0
        counted_socket.sendmsg_iovecs = 0

    state["phase"] = "measure"
    t0 = time.perf_counter()
    measure_end = t0 + duration_s
    measure_sequences = _run_publish_loop(
        client,
        state,
        topic=topic,
        qos=qos,
        body=body,
        corpus=corpus,
        run_id=run_id,
        outstanding=outstanding,
        cadence=cadence,
        until=measure_end,
        target_rate=open_loop_rate,
        properties_builder=_properties_builder(cfg, mqtt, Properties, PacketTypes),
        batch_size=int(cfg.get("batch_size", 64)) if cadence == "batch64" else 1,
        reset_sequence=True,
        force_header=bool(cfg.get("force_header", False)),
    )
    t1 = time.perf_counter()

    state["phase"] = "drain"
    drain_deadline = time.perf_counter() + drain_s
    while time.perf_counter() < drain_deadline:
        with state["lock"]:
            inflight_local = state["inflight_local"]
            pending_mids = len(state["mid_send_ns"])
        if inflight_local == 0 and pending_mids == 0:
            break
        time.sleep(0.01)

    with state["lock"]:
        backlog = state["inflight_local"]
        # QoS0 message ids are recycled aggressively; mid map leftovers are not a reliable
        # incomplete signal. Prefer the outstanding counter.
        timed_out = backlog if qos == 0 else len(state["mid_send_ns"])
        completed_in_window = state["completed_in_window"]
        completed_during_drain = state["completed_during_drain"]
        latencies = list(state["latencies_ns"])
        lags = list(state["scheduler_lags_ns"])
        counters = {
            "publish_calls": state["publish_calls"],
            "publish_accepted": state["publish_accepted"],
            "publish_rejected": state["publish_rejected"],
            "socket_completed_qos0": state["socket_completed_qos0"],
            "protocol_completed": state["protocol_completed"],
            "protocol_failed": state["protocol_failed"],
            "mid_map_remaining": len(state["mid_send_ns"]),
        }

    client.disconnect()
    client.loop_stop()

    window = max(t1 - t0, 1e-9)
    payload_len = 0 if body is None else len(body if isinstance(body, (bytes, bytearray)) else str(body).encode())
    result = {
        "ok": True,
        "role": "publisher",
        "paho_file": paho_file,
        "pid": os.getpid(),
        "topic": topic,
        "qos": qos,
        "payload": payload_name,
        "payload_bytes": payload_len,
        "cadence": cadence,
        "t0_s": t0,
        "t1_s": t1,
        "duration_s": window,
        "completed_in_window": completed_in_window,
        "completed_during_drain": completed_during_drain,
        "backlog_at_end": backlog,
        "timed_out": timed_out,
        "sent_sequence_start": measure_sequences[0] if measure_sequences else None,
        "sent_sequence_end": measure_sequences[-1] if measure_sequences else None,
        "sent_sequence_count": len(measure_sequences),
        "sent_sequences": measure_sequences if len(measure_sequences) <= 20000 else None,
        "msgs_per_s": completed_in_window / window,
        "payload_bytes_per_s": (completed_in_window * payload_len) / window,
        "latencies_ns": latencies[:50000],
        "scheduler_lags_ns": lags[:50000],
        "gc_count_start": list(gc_start),
        "gc_count_end": list(gc.get_count()),
        "socket_send_calls": counted_socket.send_calls if counted_socket is not None else None,
        "socket_sendmsg_calls": counted_socket.sendmsg_calls if counted_socket is not None else None,
        "socket_sendmsg_iovecs": counted_socket.sendmsg_iovecs if counted_socket is not None else None,
        **counters,
    }
    write_json(cfg["result_path"], result)
    return 0


def _properties_builder(cfg, mqtt, Properties, PacketTypes):
    profile = cfg.get("properties_profile", "none")
    if cfg.get("protocol") != "MQTTv5" or profile in (None, "none"):
        return lambda: None

    def build():
        props = Properties(PacketTypes.PUBLISH)
        if profile == "realistic":
            props.PayloadFormatIndicator = 1
            props.ContentType = "application/json"
            props.MessageExpiryInterval = 60
            props.UserProperty = [("schema", "telemetry.v1"), ("region", "eu-west-1")]
        elif profile == "rich":
            props.PayloadFormatIndicator = 1
            props.ContentType = "application/json"
            props.MessageExpiryInterval = 60
            props.CorrelationData = b"c" * 32
            props.ResponseTopic = "bench/response/" + ("r" * 48)
            props.UserProperty = [(f"k{i:02d}", "v" * 64) for i in range(16)]
        return props

    return build


def _run_publish_loop(
    client,
    state,
    *,
    topic,
    qos,
    body,
    corpus,
    run_id,
    outstanding,
    cadence,
    until,
    target_rate,
    properties_builder,
    batch_size=1,
    reset_sequence=False,
    force_header=False,
    sequence_start=0,
):
    sequence = sequence_start
    sent_sequences = []
    loop_start = time.perf_counter()
    next_send = loop_start
    interval = (1.0 / target_rate) if target_rate and target_rate > 0 else 0.0
    corpus_i = 0
    # reset_sequence currently means "start measure sequences at 1"; always local counter.

    while time.perf_counter() < until:
        if cadence in ("burst", "microburst"):
            # Duty-cycled capacity: 100ms burst per 1s (burst) or 10ms per 100ms (microburst).
            period, duty = (1.0, 0.1) if cadence == "burst" else (0.1, 0.01)
            phase = (time.perf_counter() - loop_start) % period
            if phase > duty:
                time.sleep(min(0.001, period - phase))
                continue
        # Closed-loop outstanding gate.
        with state["lock"]:
            inflight_local = state["inflight_local"]
        if cadence == "capacity" or target_rate is None:
            if inflight_local >= outstanding:
                time.sleep(0.0001)
                continue
        else:
            now = time.perf_counter()
            if now < next_send:
                time.sleep(min(0.001, next_send - now))
                continue
            lag_ns = int((now - next_send) * 1e9)
            with state["lock"]:
                state["scheduler_lags_ns"].append(lag_ns)
            next_send += interval
            # Do not skip slots (no coordinated omission).

        n = batch_size if cadence == "batch64" else 1
        for _ in range(n):
            if time.perf_counter() >= until:
                break
            with state["lock"]:
                if state["inflight_local"] >= outstanding and (cadence == "capacity" or target_rate is None):
                    break
            sequence += 1
            send_ns = time.perf_counter_ns()
            header = encode_header(run_id, 1, sequence, sequence, send_ns)
            if corpus:
                payload_body = corpus[corpus_i % len(corpus)]
                corpus_i += 1
            else:
                payload_body = body
            if isinstance(payload_body, str):
                # Keep str path for telemetry256_str unless integrity header is required.
                if force_header:
                    raw = payload_body.encode("utf-8")
                    payload = wrap_with_header(raw if len(raw) >= HEADER_SIZE else header + raw, header)
                else:
                    payload = payload_body
            else:
                if force_header and len(payload_body) < HEADER_SIZE:
                    payload = header
                elif len(payload_body) >= HEADER_SIZE:
                    payload = wrap_with_header(payload_body, header)
                elif len(payload_body) == 0:
                    payload = header if force_header else b""
                else:
                    payload = payload_body

            props = properties_builder()
            state["publish_calls"] += 1
            info = client.publish(topic, payload=payload, qos=qos, retain=False, properties=props)
            if info.rc == 0:
                state["publish_accepted"] += 1
                sent_sequences.append(sequence)
                with state["lock"]:
                    state["inflight_local"] += 1
                    state["mid_send_ns"][info.mid] = send_ns
            else:
                state["publish_rejected"] += 1
    return sent_sequences


if __name__ == "__main__":
    raise SystemExit(main())
