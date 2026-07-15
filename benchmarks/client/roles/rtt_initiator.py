"""RTT initiator: publishes requests and measures response latency."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import threading
import time
from pathlib import Path

ROLE_DIR = Path(__file__).resolve().parent
CLIENT_DIR = ROLE_DIR.parent
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

from control import barrier_client_wait, configure_source_root, touch, write_json  # noqa: E402
from workloads import encode_header, decode_header, HEADER_SIZE  # noqa: E402


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    paho_file = configure_source_root(cfg["source_root"])
    import paho.mqtt.client as mqtt

    request_topic = cfg["request_topic"]
    response_topic = cfg["response_topic"]
    qos = int(cfg.get("qos_publish", 1))
    duration_s = float(cfg.get("duration_s", 3))
    warmup_s = float(cfg.get("warmup_s", 1))
    drain_s = float(cfg.get("drain_s", 2))
    outstanding = int(cfg.get("outstanding", 32))
    cadence = str(cfg.get("cadence", "capacity"))
    if cadence == "capacity":
        target_rate = None
    elif cfg.get("target_rate") is not None:
        target_rate = float(cfg["target_rate"])
    else:
        write_json(cfg["result_path"], {"ok": False, "error": "open_loop_without_target_rate", "paho_file": paho_file})
        return 1
    run_id = cfg["run_id"].encode("ascii")

    state = {
        "connected": threading.Event(),
        "subscribed": threading.Event(),
        "phase": "init",
        "inflight": {},
        "latencies_ns": [],
        "timeouts": 0,
        "sent_in_window": 0,
        "completed_in_window": 0,
        "lock": threading.Lock(),
    }

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if int(getattr(reason_code, "value", reason_code)) == 0:
            state["connected"].set()
            client.subscribe(response_topic, qos=qos)

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        if all(int(getattr(x, "value", x)) < 128 for x in reason_code_list):
            state["subscribed"].set()

    def on_message(client, userdata, msg):
        now = time.perf_counter_ns()
        payload = msg.payload or b""
        if len(payload) < HEADER_SIZE:
            return
        try:
            hdr = decode_header(payload)
        except ValueError:
            return
        corr = hdr["correlation"]
        with state["lock"]:
            sent = state["inflight"].pop(corr, None)
            if sent is None:
                return
            if state["phase"] == "measure":
                state["latencies_ns"].append(now - sent)
                state["completed_in_window"] += 1

    protocol = getattr(mqtt, cfg.get("protocol", "MQTTv311"))
    kwargs = {
        "callback_api_version": mqtt.CallbackAPIVersion.VERSION2,
        "client_id": cfg.get("client_id", f"rtt-{cfg['run_id']}"),
        "protocol": protocol,
    }
    if cfg.get("protocol", "MQTTv311") != "MQTTv5":
        kwargs["clean_session"] = True
    client = mqtt.Client(**kwargs)

    def set_tcp_nodelay(client, userdata, sock):
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, ValueError, AttributeError):
            pass

    client.on_socket_open = set_tcp_nodelay
    client.max_inflight_messages = int(cfg.get("inflight", 20))
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    if cfg.get("tls"):
        client.tls_set(ca_certs=cfg["ca_certs"])
    client.connect(cfg["host"], int(cfg["port"]), keepalive=60)
    client.loop_start()
    if not state["connected"].wait(30) or not state["subscribed"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "ready_timeout", "paho_file": paho_file})
        client.loop_stop()
        return 1

    touch(cfg["ready_path"], {"role": "rtt_initiator", "paho_file": paho_file, "pid": os.getpid()})
    barrier_client_wait(cfg["barrier_path"], "T0", timeout_s=float(cfg.get("barrier_timeout_s", 120)))

    state["phase"] = "warmup"
    _send_loop(client, state, request_topic, qos, run_id, outstanding, target_rate, time.perf_counter() + warmup_s)
    with state["lock"]:
        state["latencies_ns"].clear()
        state["sent_in_window"] = 0
        state["completed_in_window"] = 0
        state["timeouts"] = 0

    state["phase"] = "measure"
    t0 = time.perf_counter()
    _send_loop(client, state, request_topic, qos, run_id, outstanding, target_rate, t0 + duration_s)
    t1 = time.perf_counter()
    state["phase"] = "drain"
    deadline = time.perf_counter() + drain_s
    while time.perf_counter() < deadline:
        with state["lock"]:
            if not state["inflight"]:
                break
        time.sleep(0.01)
    with state["lock"]:
        timeouts = len(state["inflight"])
        latencies = list(state["latencies_ns"])
        completed = state["completed_in_window"]
        sent = state["sent_in_window"]

    client.disconnect()
    client.loop_stop()
    window = max(t1 - t0, 1e-9)
    write_json(
        cfg["result_path"],
        {
            "ok": True,
            "role": "rtt_initiator",
            "paho_file": paho_file,
            "duration_s": window,
            "sent_in_window": sent,
            "completed_in_window": completed,
            "timeouts": timeouts,
            "failure_rate": (timeouts / sent) if sent else None,
            "latencies_ns": latencies[:50000],
            "msgs_per_s": completed / window,
        },
    )
    return 0


def _send_loop(client, state, topic, qos, run_id, outstanding, target_rate, until):
    interval = (1.0 / target_rate) if target_rate and target_rate > 0 else 0.0
    next_send = time.perf_counter()
    seq = 0
    while time.perf_counter() < until:
        with state["lock"]:
            inflight = len(state["inflight"])
        if inflight >= outstanding:
            time.sleep(0.0005)
            continue
        now = time.perf_counter()
        if interval and now < next_send:
            time.sleep(min(0.001, next_send - now))
            continue
        if interval:
            next_send += interval
        seq += 1
        send_ns = time.perf_counter_ns()
        payload = encode_header(run_id, 1, seq, seq, send_ns)
        info = client.publish(topic, payload=payload, qos=qos, retain=False)
        if info.rc == 0:
            with state["lock"]:
                state["inflight"][seq] = send_ns
                if state["phase"] == "measure":
                    state["sent_in_window"] += 1


if __name__ == "__main__":
    raise SystemExit(main())
