"""
Subscriber worker process.

Usage:
  python roles/subscriber.py --config /path/config.json
"""

from __future__ import annotations

import argparse
import gc
import json
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
    callback_match_topics,
    decode_header,
    fleet_topics,
    overlapping_match_filters,
    single_topic,
    unicode_topic,
    wildcard_hash,
    wildcard_plus,
    deep_topic,
    long_topic,
)


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
    with open(args.config, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)

    paho_file = configure_source_root(cfg["source_root"])
    import paho.mqtt.client as mqtt

    run_id = cfg["run_id"]
    qos = int(cfg.get("qos_subscribe", 0))
    duration_s = float(cfg.get("duration_s", 3.0))
    warmup_s = float(cfg.get("warmup_s", 1.0))
    drain_s = float(cfg.get("drain_s", 2.0))

    state = {
        "connected": threading.Event(),
        "subscribed": threading.Event(),
        "subscriber_delivered": 0,
        "delivered_in_window": 0,
        "delivered_during_drain": 0,
        "bytes_in_window": 0,
        "callback_invocations": 0,
        "sequences": [],
        "latencies_ns": [],
        "phase": "init",
        "lock": threading.Lock(),
        "sub_mids": set(),
        "granted_ok": True,
    }

    filters = _subscription_filters(cfg, run_id)
    callback_filters = int(cfg.get("callback_filters", 0) or 0)
    overlapping = bool(cfg.get("overlapping_callbacks", False))
    local_callback_topics = (
        callback_match_topics(run_id, callback_filters) if callback_filters > 0 and not overlapping else []
    )

    def _record_delivery_locked(msg, now: int) -> None:
        """Count one application delivery. Caller must hold state['lock']."""
        state["subscriber_delivered"] += 1
        if state["phase"] == "measure":
            state["delivered_in_window"] += 1
            state["bytes_in_window"] += len(msg.payload or b"")
            payload = msg.payload or b""
            if len(payload) >= HEADER_SIZE:
                try:
                    hdr = decode_header(payload)
                    state["sequences"].append(hdr["sequence"])
                    send_ns = hdr["send_ns"]
                    if send_ns:
                        state["latencies_ns"].append(now - send_ns)
                except ValueError:
                    pass
        elif state["phase"] == "drain":
            state["delivered_during_drain"] += 1
            # Integrity must still account for in-flight messages arriving
            # after T1, otherwise the window edge shows up as false "missing".
            payload = msg.payload or b""
            if len(payload) >= HEADER_SIZE:
                try:
                    state["sequences"].append(decode_header(payload)["sequence"])
                except ValueError:
                    pass

    def on_connect(client, userdata, flags, reason_code, properties=None):
        rc = int(getattr(reason_code, "value", reason_code))
        if rc != 0:
            return
        state["connected"].set()
        for filt in filters:
            result, mid = client.subscribe(filt, qos=qos)
            if result == mqtt.MQTT_ERR_SUCCESS and mid is not None:
                state["sub_mids"].add(mid)

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        # reason_code_list may be ints (v3) or ReasonCode (v5)
        ok = True
        for item in reason_code_list:
            code = int(getattr(item, "value", item))
            if code >= 128:
                ok = False
        with state["lock"]:
            state["sub_mids"].discard(mid)
            state["granted_ok"] = state["granted_ok"] and ok
            if not state["sub_mids"] and state["granted_ok"]:
                state["subscribed"].set()

    def on_message(client, userdata, msg):
        # Used when no topic-specific callback matches.
        now = time.perf_counter_ns()
        with state["lock"]:
            state["callback_invocations"] += 1
            _record_delivery_locked(msg, now)

    protocol = getattr(mqtt, cfg.get("protocol", "MQTTv311"))
    client_kwargs = {
        "callback_api_version": mqtt.CallbackAPIVersion.VERSION2,
        "client_id": cfg.get("client_id", f"sub-{run_id}"),
        "protocol": protocol,
    }
    if cfg.get("protocol", "MQTTv311") != "MQTTv5":
        client_kwargs["clean_session"] = not bool(cfg.get("session_persistent", False))
    client = mqtt.Client(**client_kwargs)
    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    counted_socket = None
    if cfg.get("count_socket_writes"):
        def on_socket_open(opened_client, userdata, sock):
            nonlocal counted_socket
            counted_socket = _CountingSocket(sock)
            opened_client._sock = counted_socket

        client.on_socket_open = on_socket_open

    # Local callback matching.
    # Paho skips on_message when at least one message_callback_add filter matches,
    # so filtered callbacks must record deliveries themselves.
    if callback_filters > 0:
        if overlapping:
            # Distinct filters that all match the same published topics.
            # (Paho keeps one callback per filter string; duplicates would overwrite.)
            for i, filt in enumerate(overlapping_match_filters(run_id, callback_filters)):
                count_delivery = i == 0

                def _cb(client, userdata, msg, _count_delivery=count_delivery):
                    now = time.perf_counter_ns()
                    with state["lock"]:
                        state["callback_invocations"] += 1
                        if _count_delivery:
                            _record_delivery_locked(msg, now)

                client.message_callback_add(filt, _cb)
        else:
            # One disjoint exact filter per callback; traffic should hit exactly one.
            for filt in local_callback_topics:
                def _cb(client, userdata, msg):
                    now = time.perf_counter_ns()
                    with state["lock"]:
                        state["callback_invocations"] += 1
                        _record_delivery_locked(msg, now)

                client.message_callback_add(filt, _cb)

    if cfg.get("tls"):
        client.tls_set(ca_certs=cfg["ca_certs"])

    client.connect(cfg["host"], int(cfg["port"]), keepalive=int(cfg.get("keepalive", 60)))
    client.loop_start()
    if not state["connected"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "connect_timeout", "paho_file": paho_file})
        client.loop_stop()
        return 1
    if not state["subscribed"].wait(30):
        write_json(cfg["result_path"], {"ok": False, "error": "subscribe_timeout", "paho_file": paho_file})
        client.loop_stop()
        return 1

    touch(
        cfg["ready_path"],
        {
            "role": "subscriber",
            "paho_file": paho_file,
            "pid": os.getpid(),
            "filters": filters,
            "callback_filters": callback_filters,
            "callback_topics": local_callback_topics,
            "overlapping_callbacks": overlapping,
        },
    )
    barrier_client_wait(cfg["barrier_path"], "T0", timeout_s=float(cfg.get("barrier_timeout_s", 120)))

    gc.collect()
    state["phase"] = "warmup"
    time.sleep(warmup_s)
    with state["lock"]:
        state["delivered_in_window"] = 0
        state["delivered_during_drain"] = 0
        state["bytes_in_window"] = 0
        state["sequences"].clear()
        state["latencies_ns"].clear()
        state["callback_invocations"] = 0
        state["subscriber_delivered"] = 0
    if counted_socket is not None:
        counted_socket.send_calls = 0
        counted_socket.sendmsg_calls = 0
        counted_socket.sendmsg_iovecs = 0

    state["phase"] = "measure"
    t0 = time.perf_counter()
    time.sleep(duration_s)
    t1 = time.perf_counter()

    # Snapshot measure-window counters before drain so rates match duration_s.
    with state["lock"]:
        delivered = state["delivered_in_window"]
        bytes_in_window = state["bytes_in_window"]
        sequences = list(state["sequences"])
        latencies = list(state["latencies_ns"])
        callback_invocations = state["callback_invocations"]

    state["phase"] = "drain"
    time.sleep(drain_s)

    with state["lock"]:
        during_drain = state["delivered_during_drain"]
        # Include drain-phase sequences for integrity accounting.
        sequences = list(state["sequences"])

    client.disconnect()
    client.loop_stop()

    window = max(t1 - t0, 1e-9)
    result = {
        "ok": True,
        "role": "subscriber",
        "paho_file": paho_file,
        "pid": os.getpid(),
        "filters": filters,
        "callback_filters": callback_filters,
        "callback_topics_count": len(local_callback_topics),
        "overlapping_callbacks": overlapping,
        "qos": qos,
        "t0_s": t0,
        "t1_s": t1,
        "duration_s": window,
        "subscriber_delivered": delivered,
        "delivered_during_drain": during_drain,
        "msgs_per_s": delivered / window,
        "callbacks_per_s": callback_invocations / window,
        "payload_bytes_in_window": bytes_in_window,
        "payload_bytes_per_s": bytes_in_window / window,
        "callback_invocations": callback_invocations,
        "sequences": sequences[:200000],
        "latencies_ns": latencies[:50000],
        "socket_send_calls": counted_socket.send_calls if counted_socket is not None else None,
        "socket_sendmsg_calls": counted_socket.sendmsg_calls if counted_socket is not None else None,
        "socket_sendmsg_iovecs": counted_socket.sendmsg_iovecs if counted_socket is not None else None,
    }
    write_json(cfg["result_path"], result)
    return 0


def _subscription_filters(cfg, run_id):
    kind = cfg.get("subscription", "exact")
    if kind == "exact":
        topo = cfg.get("topic_topology", "single")
        if topo == "deep32":
            return [deep_topic(run_id, 32)]
        if topo == "long_topic_256":
            return [long_topic(run_id, 256)]
        if topo == "long_topic_1024":
            return [long_topic(run_id, 1024)]
        if topo == "unicode":
            return [unicode_topic(run_id)]
        return [cfg.get("topic") or single_topic(run_id)]
    if kind == "plus":
        return [wildcard_plus(run_id)]
    if kind == "hash":
        return [wildcard_hash(run_id)]
    if kind == "multi_exact":
        count = int(cfg.get("subscription_count", 16))
        topics = fleet_topics(run_id)
        return topics[:count]
    return [cfg.get("topic") or single_topic(run_id)]


if __name__ == "__main__":
    raise SystemExit(main())
