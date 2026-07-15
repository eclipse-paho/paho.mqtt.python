"""Orchestration of client benchmark runs."""

from __future__ import annotations

import ast
import json
import os
import random
import secrets
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from broker import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_TLS_PORT,
    broker_container_name,
    broker_down,
    broker_up,
    ensure_certs,
    parse_broker_endpoint,
    wait_for_broker,
)
from control import BarrierServer, read_json, wait_for_file, write_json
from loadgen import EmqttBenchProcess, LoadgenSpec, interval_for_rate, nominal_rate
from metrics import (
    abba_order,
    compare_verdict,
    integrity_counts,
    latency_summary,
    median,
    sanitize_number,
    summarize_runs,
)
from network import PROFILES as NETWORK_PROFILES
from network import apply_profile, clear_profile, qdisc_stats
from scenarios import SCENARIO_BY_NAME, expand_scenario, list_scenarios, estimate_suite
from telemetry import TelemetrySampler, allocate_cpuset, environment_metadata
from workloads import (
    PAYLOAD_SPECS,
    callback_match_loadgen_topic,
    deep_topic,
    fleet_topics,
    long_topic,
    single_topic,
    unicode_topic,
    wildcard_hash,
)

CLIENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = CLIENT_DIR.parent.parent
ROLES = CLIENT_DIR / "roles"


def make_run_id() -> str:
    # Fixed 8-char ascii id to keep topic sizes stable.
    return secrets.token_hex(4)


def source_identity(source_root: str) -> dict:
    """Record the exact source module and declared version used by workers."""
    module = Path(source_root).resolve() / "src" / "paho" / "mqtt" / "__init__.py"
    version = None
    try:
        tree = ast.parse(module.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets):
                version = ast.literal_eval(node.value)
                break
    except (OSError, SyntaxError, ValueError):
        pass
    return {"module": str(module), "version": version}


def _python() -> str:
    return sys.executable


def _spawn_role(script: str, config_path: str, cpuset: Optional[str] = None) -> subprocess.Popen:
    cmd = [_python(), str(ROLES / script), "--config", config_path]
    env = os.environ.copy()
    # Prevent accidental imports from ambient site-packages overshadowing source_root.
    env.setdefault("PYTHONNOUSERSITE", "1")
    preexec = None
    if cpuset and hasattr(os, "sched_setaffinity"):
        cpus = {int(x) for x in cpuset.split(",") if x.strip() != ""}

        def _set_affinity():
            os.sched_setaffinity(0, cpus)

        preexec = _set_affinity
    return subprocess.Popen(cmd, env=env, preexec_fn=preexec)


def unsupported_features(point: dict) -> List[str]:
    """Scenario knobs declared in the catalogue but not implemented by the harness.

    Points using them are refused up front instead of silently measuring
    something else than what the point claims.
    """
    missing = []
    if point.get("receive_maximum") is not None:
        missing.append("receive_maximum")
    if point.get("retained_count") is not None:
        missing.append("retained_count")
    if point.get("outage_s") is not None:
        missing.append("session_outage")
    if point.get("submit_count") is not None:
        missing.append("queue_rejection_protocol")
    if point.get("properties_profile") in ("topic_alias", "subscription_identifier"):
        missing.append(f"properties_profile:{point['properties_profile']}")
    if point.get("connect_mode") in ("tls_resume", "tcp_concurrent"):
        missing.append(f"connect_mode:{point['connect_mode']}")
    if str(point.get("topic_topology", "")) in ("fleet4k_zipf", "fleet100k"):
        # Loadgen publishes on a single fixed topic; cardinality/skew is not offered.
        missing.append(f"topic_topology:{point['topic_topology']}")
    if point.get("integrity") and point.get("topology") == "publisher_only":
        missing.append("integrity_without_oracle")
    return missing


def validate_run(point: dict, worker_results: List[dict], loadgen_stats: Optional[dict], telemetry_samples: List[dict]) -> dict:
    reasons = []
    for result in worker_results:
        if not result.get("ok", False):
            reasons.append(f"worker_error:{result.get('error', 'unknown')}")
        if result.get("protocol_failed"):
            reasons.append("protocol_failed")
        timed_out = int(result.get("timed_out") or 0)
        completed = int(result.get("completed_in_window") or 0)
        backlog = int(result.get("backlog_at_end") or 0)
        # A few in-flight leftovers after a short drain are noise; flag only material backlog.
        if timed_out > 64 and (completed == 0 or timed_out / max(completed, 1) > 0.01 or backlog > 64):
            reasons.append("timed_out_mids")
        if result.get("role") == "rtt_initiator":
            sent = int(result.get("sent_in_window") or 0)
            timeouts = int(result.get("timeouts") or 0)
            if timeouts > 0 and (sent == 0 or timeouts / max(sent, 1) > 0.01):
                reasons.append("rtt_timeouts")

    # Open-loop charge adherence.
    if point.get("cadence") in ("steady50", "loaded75", "loaded90", "periodic10") and point.get("target_rate"):
        for result in worker_results:
            if result.get("role") in ("publisher", "rtt_initiator") and result.get("msgs_per_s") is not None:
                target = float(point["target_rate"])
                actual = float(result["msgs_per_s"])
                if target > 0 and abs(actual - target) / target > 0.02:
                    # For capacity-limited machines this often trips; keep as inconclusive signal.
                    reasons.append("open_loop_rate_out_of_tolerance")

    # An ingress run where the loadgen emitted traffic but nothing was delivered
    # indicates a topic/filter mismatch or a broken subscriber, not a Paho score.
    if point.get("topology") == "subscriber_ingress":
        emitted = ((loadgen_stats or {}).get("parsed") or {}).get("last_total")
        delivered = sum(int(r.get("subscriber_delivered") or 0) for r in worker_results if r.get("role") == "subscriber")
        if emitted and delivered == 0:
            reasons.append("no_delivery_despite_load")

    # Telemetry saturation heuristics.
    for sample in telemetry_samples[-5:]:
        for name, stats in (sample.get("containers") or {}).items():
            if stats and stats.get("cpu_pct") is not None and stats["cpu_pct"] >= 85.0:
                reasons.append(f"container_cpu_high:{name}")
    watched_any = False
    watched_ok = False
    for sample in telemetry_samples:
        for stats in (sample.get("containers") or {}).values():
            watched_any = True
            if stats is not None:
                watched_ok = True
    if watched_any and not watched_ok:
        reasons.append("broker_telemetry_missing")

    if loadgen_stats and loadgen_stats.get("parsed") and point.get("cadence") not in ("burst", "microburst"):
        parsed = loadgen_stats["parsed"]
        nominal = loadgen_stats.get("nominal_rate")
        last = parsed.get("last_rate")
        if nominal and last is not None and nominal < float("inf"):
            if last < 0.5 * nominal:
                reasons.append("loadgen_below_half_nominal")

    status = "valid" if not reasons else "inconclusive"
    bottleneck = "bottleneck_unattributed"
    if any(r.startswith("container_cpu_high:") and "mosquitto" in r for r in reasons):
        bottleneck = "broker_limited"
    elif any(r.startswith("loadgen_") for r in reasons):
        bottleneck = "loadgen_limited"
    elif not reasons:
        bottleneck = "sut_limited"

    return {"status": status, "reasons": reasons, "bottleneck": bottleneck}


def run_point(
    point: dict,
    *,
    source_root: str,
    host: str,
    port: int,
    tls_port: int,
    profile: str,
    work_dir: Path,
    cpusets: Dict[str, str],
    load_profile: Optional[dict] = None,
    managed_broker: bool = True,
) -> dict:
    run_id = make_run_id()
    point = dict(point)
    point["run_id"] = run_id

    missing = unsupported_features(point)
    if missing:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "status": "inconclusive",
            "reasons": [f"not_implemented:{m}" for m in missing],
            "workers": [],
        }

    if load_profile and point.get("load_fraction") is not None:
        capacity = (
            load_profile.get("rtt_capacity_msgs_per_s")
            if point.get("topology") == "application_rtt"
            else load_profile.get("capacity_msgs_per_s")
        )
        if capacity:
            point["target_rate"] = float(capacity) * float(point["load_fraction"])
            point["calibration_kind"] = "rtt" if point.get("topology") == "application_rtt" else "publish"
    if point.get("load_fraction") is not None and not point.get("target_rate"):
        kind = "rtt" if point.get("topology") == "application_rtt" else "publish"
        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "status": "inconclusive",
            "reasons": [f"load_fraction_without_{kind}_calibration"],
            "workers": [],
        }

    network = point.get("network", "localhost")
    net_result = apply_profile(network)
    if network != "localhost" and not net_result.get("applied"):
        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "status": "inconclusive",
            "reasons": [f"network_unavailable:{net_result.get('reason')}"],
            "network": net_result,
        }

    use_tls = bool(point.get("tls"))
    endpoint_port = tls_port if use_tls else port
    certs = ensure_certs() if use_tls else {}

    barrier_path = str(work_dir / f"barrier-{run_id}.sock")
    barrier = BarrierServer(barrier_path)

    workers = []
    configs = []
    topology = point.get("topology")
    topic = point.get("topic") or single_topic(run_id)

    def base_cfg(role: str, script_stem: str) -> dict:
        ready = str(work_dir / f"{role}-{run_id}.ready")
        result = str(work_dir / f"{role}-{run_id}.json")
        cfg = {
            "source_root": source_root,
            "run_id": run_id,
            "host": host,
            "port": endpoint_port,
            "tls": use_tls,
            "ca_certs": certs.get("ca_crt"),
            "ready_path": ready,
            "result_path": result,
            "barrier_path": barrier_path,
            "barrier_timeout_s": 180,
            "topic": topic,
            **{k: point.get(k) for k in (
                "qos_publish", "qos_subscribe", "payload", "cadence", "inflight", "max_queued",
                "outstanding", "duration_s", "warmup_s", "drain_s", "protocol", "properties_profile",
                "load_fraction", "target_rate", "session_persistent", "callback_filters",
                "overlapping_callbacks", "subscription", "topic_topology", "subscription_count",
                "keepalive", "batch_size",
                "count_socket_writes",
            ) if k in point or point.get(k) is not None},
        }
        # Fill defaults from point always.
        for key, default in (
            ("qos_publish", 0),
            ("qos_subscribe", 0),
            ("payload", "telemetry256"),
            ("cadence", "capacity"),
            ("inflight", 20),
            ("max_queued", 200),
            ("outstanding", 64),
            ("duration_s", 3.0 if profile == "smoke" else 60.0),
            ("warmup_s", 1.0 if profile == "smoke" else 15.0),
            ("drain_s", 2.0 if profile == "smoke" else 30.0),
            ("protocol", "MQTTv311"),
            ("force_header", False),
        ):
            cfg.setdefault(key, point.get(key, default))
        if "force_header" in point:
            cfg["force_header"] = point["force_header"]
        return cfg

    loadgen = None
    loadgen_stats = None
    expected_workers = 0

    try:
        if topology == "publisher_only":
            cfg = base_cfg("publisher", "publisher")
            cfg_path = work_dir / f"publisher-{run_id}.cfg.json"
            write_json(str(cfg_path), cfg)
            workers.append(_spawn_role("publisher.py", str(cfg_path), cpusets.get("sut")))
            configs.append(cfg)
            expected_workers = 1

        elif topology in ("publisher_with_oracle", "fanout"):
            n_sub = int(point.get("subscribers", 1) or 1)
            pub_cfg = base_cfg("publisher", "publisher")
            pub_path = work_dir / f"publisher-{run_id}.cfg.json"
            write_json(str(pub_path), pub_cfg)
            workers.append(_spawn_role("publisher.py", str(pub_path), cpusets.get("sut")))
            configs.append(pub_cfg)
            for i in range(n_sub):
                sub_cfg = base_cfg(f"subscriber{i}", "subscriber")
                sub_cfg["client_id"] = f"sub{i}-{run_id}"
                sub_cfg["qos_subscribe"] = point.get("qos_subscribe", point.get("qos_publish", 0))
                sub_path = work_dir / f"subscriber{i}-{run_id}.cfg.json"
                write_json(str(sub_path), sub_cfg)
                workers.append(_spawn_role("subscriber.py", str(sub_path), cpusets.get("sut")))
                configs.append(sub_cfg)
            expected_workers = 1 + n_sub

        elif topology == "subscriber_ingress":
            sub_cfg = base_cfg("subscriber", "subscriber")
            sub_path = work_dir / f"subscriber-{run_id}.cfg.json"
            write_json(str(sub_path), sub_cfg)
            workers.append(_spawn_role("subscriber.py", str(sub_path), cpusets.get("sut")))
            configs.append(sub_cfg)
            expected_workers = 1
            # Start loadgen after subscriber ready.

        elif topology == "application_rtt":
            req = f"bench/{run_id}/rtt/request"
            resp = f"bench/{run_id}/rtt/response"
            resp_cfg = base_cfg("responder", "responder")
            resp_cfg.update({"request_topic": req, "response_topic": resp})
            resp_path = work_dir / f"responder-{run_id}.cfg.json"
            write_json(str(resp_path), resp_cfg)
            workers.append(_spawn_role("responder.py", str(resp_path), cpusets.get("orch")))
            configs.append(resp_cfg)

            init_cfg = base_cfg("rtt", "rtt_initiator")
            init_cfg.update({"request_topic": req, "response_topic": resp})
            init_path = work_dir / f"rtt-{run_id}.cfg.json"
            write_json(str(init_path), init_cfg)
            workers.append(_spawn_role("rtt_initiator.py", str(init_path), cpusets.get("sut")))
            configs.append(init_cfg)
            expected_workers = 2

        elif topology == "duplex_gateway":
            # SUT publishes telemetry while a SUT subscriber receives commands
            # injected by emqtt-bench (two client processes on the sut cpuset).
            sub_cfg = base_cfg("subscriber", "subscriber")
            sub_cfg["subscription"] = "exact"
            sub_cfg["topic"] = f"bench/{run_id}/commands"
            sub_path = work_dir / f"gateway-sub-{run_id}.cfg.json"
            write_json(str(sub_path), sub_cfg)
            workers.append(_spawn_role("subscriber.py", str(sub_path), cpusets.get("sut")))
            configs.append(sub_cfg)
            pub_cfg = base_cfg("publisher", "publisher")
            pub_cfg["topic"] = f"bench/{run_id}/telemetry"
            pub_path = work_dir / f"gateway-pub-{run_id}.cfg.json"
            write_json(str(pub_path), pub_cfg)
            workers.append(_spawn_role("publisher.py", str(pub_path), cpusets.get("sut")))
            configs.append(pub_cfg)
            expected_workers = 2

        elif topology == "connect":
            # Lightweight in-orchestrator connect probe using a child publisher with duration 0 replaced.
            result = _run_connect_churn(point, source_root, host, endpoint_port, use_tls, certs)
            return {
                "schema_version": 1,
                "run_id": run_id,
                "point": point,
                "status": "valid" if result.get("ok") else "inconclusive",
                "reasons": [] if result.get("ok") else ["connect_failed"],
                "workers": [result],
                "managed_broker": managed_broker,
                "environment": environment_metadata(),
            }

        elif topology == "fleet":
            result = _run_fleet_idle(point, source_root, host, endpoint_port, use_tls, certs)
            return {
                "schema_version": 1,
                "run_id": run_id,
                "point": point,
                "status": "valid" if result.get("ok") else "inconclusive",
                "reasons": [] if result.get("ok") else ["fleet_failed"],
                "workers": [result],
                "managed_broker": managed_broker,
                "environment": environment_metadata(),
            }

        else:
            return {
                "schema_version": 1,
                "run_id": run_id,
                "point": point,
                "status": "inconclusive",
                "reasons": [f"unsupported_topology:{topology}"],
            }

        # Wait for ready files.
        for cfg in configs:
            wait_for_file(cfg["ready_path"], timeout_s=60.0)

        cadence = str(point.get("cadence", "capacity"))
        burst_ingress = topology == "subscriber_ingress" and cadence in ("burst", "microburst")

        if topology == "subscriber_ingress":
            clients = int(point.get("loadgen_clients", 32) or 32)
            payload = point.get("payload", "telemetry256")
            size = PAYLOAD_SPECS.get(payload, {"size": 256})["size"]
            # Capacity points must exceed the historical ~5k delivery ceiling
            # even in smoke runs, otherwise A/B ingress optimisations are hidden
            # behind the offered rate and incorrectly labelled SUT-limited.
            target = float(point.get("target_rate") or 40000.0)
            if point.get("fanin_mode") == "per_publisher":
                target = clients * 1000.0
            if cadence == "periodic10":
                target = 10.0
            callback_filters = int(point.get("callback_filters", 0) or 0)
            overlapping = bool(point.get("overlapping_callbacks", False))
            lg_topic = topic
            if callback_filters > 0:
                # Publish onto cb/%i/data so local message_callback_add filters receive traffic.
                lg_topic = callback_match_loadgen_topic(run_id)
                if not overlapping:
                    # Keep the client count (and thus offered load) comparable across
                    # variants: every message goes through iter_match; messages whose
                    # cb/<i> topic has no registered filter fall back to on_message,
                    # which also records the delivery. Cap avoids a connection storm.
                    clients = max(clients, min(callback_filters, 256))
                # Keep aggregate offered load stable when client count grows with filters.
                target = float(point.get("target_rate") or 40000.0)
            elif point.get("subscription") in ("plus", "hash") or str(point.get("topic_topology", "")).startswith("fleet"):
                lg_topic = f"bench/{run_id}/org/acme/site/s0000/device/d0000/telemetry/temperature"
            else:
                # Exact-subscription stress topologies: publish on the same topic
                # the subscriber registered, or nothing gets delivered.
                topo = str(point.get("topic_topology", "single"))
                if topo == "deep32":
                    lg_topic = deep_topic(run_id, 32)
                elif topo == "long_topic_256":
                    lg_topic = long_topic(run_id, 256)
                elif topo == "long_topic_1024":
                    lg_topic = long_topic(run_id, 1024)
                elif topo == "unicode":
                    lg_topic = unicode_topic(run_id)
            limit_total = 0
            interval = interval_for_rate(clients, target)
            if burst_ingress:
                # Offer a bounded burst at max speed, then silence; the subscriber's
                # window rate plus delivered_during_drain expose backlog recovery.
                # emqtt-bench -L is a global cap across all clients.
                limit_total = 1000 if cadence == "microburst" else max(1, int(target * float(point.get("duration_s", 3))))
                interval = 1
            spec = LoadgenSpec(
                host=host,
                port=endpoint_port,
                topic=lg_topic,
                qos=int(point.get("qos_publish", 0)),
                clients=clients,
                interval_ms=interval,
                payload_size=max(size, 1),
                duration_s=float(point.get("duration_s", 3)),
                limit=limit_total,
            )
            loadgen = EmqttBenchProcess(spec, cpuset=cpusets.get("loadgen"))
            if not burst_ingress:
                loadgen.start()

        elif topology == "duplex_gateway":
            # Modest command stream toward the SUT subscriber while the SUT publishes.
            spec = LoadgenSpec(
                host=host,
                port=endpoint_port,
                topic=f"bench/{run_id}/commands",
                qos=int(point.get("qos_subscribe", 1)),
                clients=2,
                interval_ms=interval_for_rate(2, 200.0),
                payload_size=256,
                duration_s=float(point.get("duration_s", 3)),
            )
            loadgen = EmqttBenchProcess(spec, cpuset=cpusets.get("loadgen"))
            loadgen.start()

        barrier.accept_n(expected_workers, timeout_s=60.0)
        if loadgen is not None and loadgen.proc is not None:
            # Let the loadgen connect ramp finish outside the measure window.
            ramp_s = min(loadgen.spec.clients * loadgen.spec.connect_interval_ms / 1000.0 + 0.5, 15.0)
            time.sleep(ramp_s)
        sampler = TelemetrySampler(
            pids={f"w{i}": w.pid for i, w in enumerate(workers) if w.pid},
            containers=[broker_container_name()] if managed_broker else [],
        )
        sampler.start()
        barrier.broadcast("T0")
        if burst_ingress and loadgen is not None:
            # Burst arrives inside the measure window rather than during connect ramp.
            loadgen.start()

        # Wait workers; a hung worker invalidates the run instead of crashing the harness.
        worker_hang = False
        worker_timeout = max(120.0, float(point.get("duration_s", 3)) + float(point.get("warmup_s", 1)) + float(point.get("drain_s", 2)) + 60)
        for w in workers:
            try:
                w.wait(timeout=worker_timeout)
            except subprocess.TimeoutExpired:
                worker_hang = True
                w.kill()

        telemetry_samples = sampler.stop()
        if loadgen is not None:
            loadgen_stats = loadgen.stop()

        worker_results = []
        for cfg in configs:
            if os.path.exists(cfg["result_path"]):
                worker_results.append(read_json(cfg["result_path"]))
            else:
                worker_results.append({"ok": False, "error": "missing_result", "result_path": cfg["result_path"]})

        validity = validate_run(point, worker_results, loadgen_stats, telemetry_samples)
        if worker_hang:
            validity["status"] = "inconclusive"
            validity["reasons"].append("worker_hang")

        # Integrity enrichment when sequences present.
        pub = next((w for w in worker_results if w.get("role") == "publisher"), None)
        for wr in worker_results:
            if wr.get("role") == "subscriber" and wr.get("sequences"):
                # Warmup traffic uses a disjoint sequence range (>= 2^40); late
                # warmup deliveries are not integrity errors.
                seqs = [s for s in wr["sequences"] if s < (1 << 40)]
                expected = None
                if pub and pub.get("sent_sequences"):
                    expected = pub["sent_sequences"]
                elif pub and pub.get("sent_sequence_start") is not None and pub.get("sent_sequence_end") is not None:
                    expected = range(int(pub["sent_sequence_start"]), int(pub["sent_sequence_end"]) + 1)
                if expected is not None:
                    wr["integrity"] = integrity_counts(expected, seqs)
                elif seqs:
                    wr["integrity"] = integrity_counts(range(min(seqs), max(seqs) + 1), seqs)

        # Latency summaries.
        for wr in worker_results:
            if wr.get("latencies_ns"):
                wr["latency_summary"] = latency_summary(wr["latencies_ns"])
                # Keep raw samples only in smoke/debug; truncate in stored summary pointer already.

        primary_rate = None
        for wr in worker_results:
            if wr.get("msgs_per_s") is not None and wr.get("role") in ("publisher", "subscriber", "rtt_initiator"):
                # Prefer SUT role.
                if topology == "subscriber_ingress" and wr.get("role") == "subscriber":
                    primary_rate = wr["msgs_per_s"]
                    break
                if topology != "subscriber_ingress" and wr.get("role") in ("publisher", "rtt_initiator"):
                    primary_rate = wr["msgs_per_s"]
                    break
                primary_rate = wr["msgs_per_s"]

        return {
            "schema_version": 1,
            "run_id": run_id,
            "point": point,
            "status": validity["status"],
            "reasons": validity["reasons"],
            "bottleneck": validity["bottleneck"],
            "primary_msgs_per_s": sanitize_number(primary_rate),
            "workers": worker_results,
            "loadgen": loadgen_stats,
            "telemetry": telemetry_samples[-30:],
            "network": net_result,
            "qdisc": qdisc_stats() if network != "localhost" else None,
            "managed_broker": managed_broker,
            "environment": environment_metadata(),
            "cpusets": cpusets,
            "non_comparable": bool(point.get("non_comparable")),
        }
    finally:
        barrier.close()
        for w in workers:
            if w.poll() is None:
                w.terminate()
        if loadgen is not None and loadgen.proc is not None and loadgen.proc.poll() is None:
            loadgen.stop()
        if network != "localhost":
            clear_profile()


def _run_connect_churn(point, source_root, host, port, tls, certs) -> dict:
    from control import configure_source_root

    paho_file = configure_source_root(source_root)
    import paho.mqtt.client as mqtt

    mode = point.get("connect_mode", "tcp_serial")
    count = int(point.get("connect_count", 100))
    latencies = []
    ok = 0
    for i in range(count):
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"conn-{i}-{make_run_id()}",
            protocol=mqtt.MQTTv311,
        )
        if tls or mode.startswith("tls"):
            client.tls_set(ca_certs=certs["ca_crt"])
        connected = {"ok": False}

        def on_connect(c, u, f, rc, p=None):
            if int(getattr(rc, "value", rc)) == 0:
                connected["ok"] = True

        client.on_connect = on_connect
        t0 = time.perf_counter_ns()
        try:
            client.connect(host, port, keepalive=30)
            client.loop_start()
            deadline = time.time() + 5
            while time.time() < deadline and not connected["ok"]:
                time.sleep(0.001)
            t1 = time.perf_counter_ns()
            if connected["ok"]:
                ok += 1
                latencies.append(t1 - t0)
            client.disconnect()
            client.loop_stop()
        except Exception as exc:  # noqa: BLE001
            client.loop_stop()
            return {"ok": False, "error": str(exc), "paho_file": paho_file, "mode": mode}
    return {
        "ok": ok == count,
        "role": "connect",
        "mode": mode,
        "connect_count": count,
        "successes": ok,
        "latencies_ns": latencies,
        "latency_summary": latency_summary(latencies),
        "paho_file": paho_file,
    }


def _run_fleet_idle(point, source_root, host, port, tls, certs) -> dict:
    from control import configure_source_root

    paho_file = configure_source_root(source_root)
    import paho.mqtt.client as mqtt
    import resource

    n = int(point.get("fleet_size", 1))
    keepalive = int(point.get("keepalive", 30))
    clients = []
    for i in range(n):
        c = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"fleet-{i}-{make_run_id()}",
            protocol=mqtt.MQTTv311,
        )
        if tls:
            c.tls_set(ca_certs=certs["ca_crt"])
        c.connect(host, port, keepalive=keepalive)
        c.loop_start()
        clients.append(c)
    time.sleep(float(point.get("duration_s", 3)))
    usage = resource.getrusage(resource.RUSAGE_SELF)
    for c in clients:
        c.disconnect()
        c.loop_stop()
    return {
        "ok": True,
        "role": "fleet",
        "fleet_size": n,
        "ru_maxrss_kb": getattr(usage, "ru_maxrss", None),
        "paho_file": paho_file,
    }


def run_scenario(
    name: str,
    *,
    source_root: str,
    profile: str = "standard",
    runs: Optional[int] = None,
    broker: Optional[str] = None,
    network: Optional[str] = None,
    output: Optional[str] = None,
    load_profile_path: Optional[str] = None,
    seed: int = 42,
) -> dict:
    scenario = SCENARIO_BY_NAME[name]
    if runs is None:
        runs = 1 if profile == "smoke" else 7
    points = expand_scenario(scenario, profile)
    if network:
        for p in points:
            p["network"] = network

    managed = broker is None
    if managed:
        meta = broker_up(wait=True)
        host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]
    else:
        host, port = parse_broker_endpoint(broker)
        tls_port = DEFAULT_TLS_PORT
        wait_for_broker(host, port, timeout_s=10)
        meta = {"managed_broker": False, "host": host, "port": port, "tls_port": tls_port}

    try:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    except RuntimeError as exc:
        if profile == "standard":
            raise
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")

    load_profile = read_json(load_profile_path) if load_profile_path else None
    rng = random.Random(seed)
    ordered_points = list(points)
    rng.shuffle(ordered_points)

    all_results = []
    with tempfile.TemporaryDirectory(prefix="paho-bench-") as tmp:
        work_dir = Path(tmp)
        for point in ordered_points:
            point_runs = []
            for run_idx in range(runs):
                result = run_point(
                    point,
                    source_root=source_root,
                    host=host,
                    port=port,
                    tls_port=tls_port,
                    profile=profile,
                    work_dir=work_dir,
                    cpusets=cpusets,
                    load_profile=load_profile,
                    managed_broker=managed,
                )
                result["run_index"] = run_idx
                point_runs.append(result)
            rates = [r.get("primary_msgs_per_s") for r in point_runs if r.get("primary_msgs_per_s") is not None]
            all_results.append(
                {
                    "point": point,
                    "runs": point_runs,
                    "summary": summarize_runs([r for r in rates if r is not None]),
                }
            )

    if managed:
        # Leave broker up for consecutive scenarios; caller may down explicitly.
        pass

    payload = {
        "schema_version": 1,
        "scenario": name,
        "profile": profile,
        "runs": runs,
        "seed": seed,
        "source_root": str(Path(source_root).resolve()),
        "source_identity": source_identity(source_root),
        "broker": meta,
        "results": all_results,
        "environment": environment_metadata(),
    }
    if output:
        write_json(output, payload)
    return payload


def run_suite(suite: str, **kwargs) -> dict:
    scenarios = list_scenarios(suite)
    profile = kwargs.get("profile", "standard")
    runs = kwargs.get("runs") or (1 if profile == "smoke" else 7)
    estimate = estimate_suite(suite, profile, runs)
    print(
        f"Suite {suite}: {estimate['scenarios']} scenarios, "
        f"{estimate['points']} points, {estimate['runs_per_point']} runs/point, "
        f"~{estimate['estimated_minutes']} min",
        flush=True,
    )
    outputs = []
    for scenario in scenarios:
        print(f"==> {scenario.name}", flush=True)
        outputs.append(run_scenario(scenario.name, **kwargs))
    return {"suite": suite, "estimate": estimate, "scenarios": outputs}


def _capacity_from_result(result: dict, qos: Optional[int] = None) -> Optional[float]:
    rates = []
    for block in result.get("results", []):
        point = block.get("point") or {}
        if qos is not None and int(point.get("qos_publish", -1)) != qos:
            continue
        summary = block.get("summary") or {}
        if summary.get("median") is not None:
            rates.append(float(summary["median"]))
            continue
        for run in block.get("runs") or []:
            if run.get("status") == "valid" and run.get("primary_msgs_per_s") is not None:
                rates.append(float(run["primary_msgs_per_s"]))
    return median(rates)


def _fraction_map(capacity: Optional[float]) -> dict:
    return {
        "0.25": None if capacity is None else capacity * 0.25,
        "0.50": None if capacity is None else capacity * 0.50,
        "0.75": None if capacity is None else capacity * 0.75,
        "0.90": None if capacity is None else capacity * 0.90,
    }


def calibrate(source_root: str, output: str, profile: str = "smoke") -> dict:
    """Measure independent publish and RTT capacities for open-loop fractions."""
    publish_result = run_scenario(
        "pub_qos_sweep_telemetry",
        source_root=source_root,
        profile=profile,
        runs=1,
    )
    rtt_result = run_scenario(
        "rtt_capacity_qos1",
        source_root=source_root,
        profile=profile,
        runs=1,
    )
    capacity = _capacity_from_result(publish_result, qos=1)
    rtt_capacity = _capacity_from_result(rtt_result)
    payload = {
        "schema_version": 1,
        "source_root": str(Path(source_root).resolve()),
        "capacity_msgs_per_s": capacity,
        "rtt_capacity_msgs_per_s": rtt_capacity,
        "fractions": _fraction_map(capacity),
        "rtt_fractions": _fraction_map(rtt_capacity),
        "raw": {"publish": publish_result, "rtt": rtt_result},
    }
    write_json(output, payload)
    return payload


def compare_sources(
    baseline_source: str,
    candidate_source: str,
    scenario: str,
    *,
    blocks: int = 4,
    point_index: int = 0,
    profile: str = "smoke",
    output: Optional[str] = None,
    load_profile_path: Optional[str] = None,
) -> dict:
    order = abba_order(blocks)
    managed = True
    meta = broker_up(wait=True)
    host, port, tls_port = meta["host"], meta["port"], meta["tls_port"]
    try:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile=profile)
    except RuntimeError:
        cpusets = allocate_cpuset(["sut", "broker", "loadgen", "orch"], profile="smoke")

    scenario_obj = SCENARIO_BY_NAME[scenario]
    points = expand_scenario(scenario_obj, profile)
    if point_index < 0 or point_index >= len(points):
        raise IndexError(
            "point_index {} outside scenario {!r} range 0..{}".format(
                point_index, scenario, len(points) - 1,
            )
        )
    point = points[point_index]
    load_profile = read_json(load_profile_path) if load_profile_path else None

    baseline_rates = []
    candidate_rates = []
    raw = []
    with tempfile.TemporaryDirectory(prefix="paho-bench-ab-") as tmp:
        work_dir = Path(tmp)
        for slot, label in enumerate(order):
            source = baseline_source if label == "A" else candidate_source
            result = run_point(
                point,
                source_root=source,
                host=host,
                port=port,
                tls_port=tls_port,
                profile=profile,
                work_dir=work_dir,
                cpusets=cpusets,
                load_profile=load_profile,
                managed_broker=True,
            )
            result["ab_label"] = label
            result["slot"] = slot
            raw.append(result)
            rate = result.get("primary_msgs_per_s")
            # Prefer valid runs; fall back to successful worker rates so A/A noise remains visible.
            usable = rate is not None and (
                result.get("status") == "valid"
                or (result.get("workers") and all(w.get("ok") for w in result["workers"]))
            )
            if usable:
                if label == "A":
                    baseline_rates.append(rate)
                else:
                    candidate_rates.append(rate)

    verdict = compare_verdict(baseline_rates, candidate_rates)
    payload = {
        "schema_version": 1,
        "scenario": scenario,
        "point_index": point_index,
        "point": point,
        "order": order,
        "baseline_source": str(Path(baseline_source).resolve()),
        "candidate_source": str(Path(candidate_source).resolve()),
        "baseline_identity": source_identity(baseline_source),
        "candidate_identity": source_identity(candidate_source),
        "baseline_rates": baseline_rates,
        "candidate_rates": candidate_rates,
        "verdict": verdict,
        "runs": raw,
        "environment": environment_metadata(),
    }
    if output:
        write_json(output, payload)
    return payload
