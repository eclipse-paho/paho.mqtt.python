"""
Responder worker for application RTT measurements.

Subscribes to request topic and republishes payload to response topic.
"""

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
    qos = int(cfg.get("qos_subscribe", 1))

    state = {
        "connected": threading.Event(),
        "subscribed": threading.Event(),
        "responses": 0,
        "lock": threading.Lock(),
        "sub_mid": None,
    }

    def on_connect(client, userdata, flags, reason_code, properties=None):
        if int(getattr(reason_code, "value", reason_code)) == 0:
            state["connected"].set()
            _rc, mid = client.subscribe(request_topic, qos=qos)
            state["sub_mid"] = mid

    def on_subscribe(client, userdata, mid, reason_code_list, properties=None):
        ok = True
        for item in reason_code_list:
            if int(getattr(item, "value", item)) >= 128:
                ok = False
        if ok:
            state["subscribed"].set()

    def on_message(client, userdata, msg):
        client.publish(response_topic, payload=msg.payload, qos=qos, retain=False)
        with state["lock"]:
            state["responses"] += 1

    protocol = getattr(mqtt, cfg.get("protocol", "MQTTv311"))
    kwargs = {"callback_api_version": mqtt.CallbackAPIVersion.VERSION2, "client_id": cfg.get("client_id", f"resp-{cfg['run_id']}"), "protocol": protocol}
    if cfg.get("protocol", "MQTTv311") != "MQTTv5":
        kwargs["clean_session"] = True
    client = mqtt.Client(**kwargs)

    def set_tcp_nodelay(client, userdata, sock):
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except (OSError, ValueError, AttributeError):
            pass

    client.on_socket_open = set_tcp_nodelay
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

    touch(cfg["ready_path"], {"role": "responder", "paho_file": paho_file, "pid": os.getpid()})
    barrier_client_wait(cfg["barrier_path"], "T0", timeout_s=float(cfg.get("barrier_timeout_s", 120)))
    # Stay alive for warmup+measure+drain.
    alive = float(cfg.get("warmup_s", 1)) + float(cfg.get("duration_s", 3)) + float(cfg.get("drain_s", 2)) + 2
    time.sleep(alive)
    with state["lock"]:
        responses = state["responses"]
    client.disconnect()
    client.loop_stop()
    write_json(cfg["result_path"], {"ok": True, "role": "responder", "paho_file": paho_file, "responses": responses})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
