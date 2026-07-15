"""Scenario registry for the Paho MQTT client benchmark."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class Scenario:
    name: str
    suite: str  # core | full
    tags: Tuple[str, ...]
    topology: str
    description: str
    protocol: str = "MQTTv311"
    qos_publish: int = 0
    qos_subscribe: int = 0
    payload: str = "telemetry256"
    cadence: str = "capacity"
    topic_topology: str = "single"
    subscription: str = "exact"
    inflight: int = 20
    max_queued: int = 200
    outstanding: int = 64
    publishers: int = 1
    subscribers: int = 0
    count_socket_writes: bool = False
    loadgen_clients: int = 0
    duration_s: float = 60.0
    warmup_s: float = 15.0
    drain_s: float = 30.0
    tls: bool = False
    network: str = "localhost"
    variants: Tuple[Dict[str, Any], ...] = ()
    estimated_minutes: float = 2.0

    def resolved(self, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        data = asdict(self)
        data.pop("variants", None)
        data.pop("estimated_minutes", None)
        data.pop("description", None)
        if overrides:
            data.update(overrides)
        return data


def _variants(**axes: Sequence[Any]) -> Tuple[Dict[str, Any], ...]:
    """Build one-axis-at-a-time variant dicts from the first axis that has multiple values.

    For multi-axis scenarios we pass an explicit list via Scenario.variants instead.
    """
    items: List[Dict[str, Any]] = []
    for key, values in axes.items():
        for value in values:
            items.append({key: value})
    return tuple(items)


SCENARIOS: List[Scenario] = [
    Scenario(
        name="pub_payload_sweep_qos0",
        suite="core",
        tags=("representative",),
        topology="publisher_only",
        description="Publisher-only capacity across payload sizes at QoS 0.",
        qos_publish=0,
        payload="telemetry256",
        cadence="capacity",
        topic_topology="single",
        variants=tuple(
            {"payload": p}
            for p in (
                "empty0",
                "binary64",
                "telemetry256",
                "event1k",
                "record16k",
                "block64k",
                "blob1m",
            )
        ),
        estimated_minutes=8.0,
    ),
    Scenario(
        name="pub_qos_sweep_telemetry",
        suite="core",
        tags=("representative",),
        topology="publisher_only",
        description="Publisher-only capacity for QoS 0/1/2 with telemetry payload.",
        payload="telemetry256",
        cadence="capacity",
        variants=tuple({"qos_publish": q} for q in (0, 1, 2)),
        estimated_minutes=4.0,
    ),
    Scenario(
        name="pub_payload_16k",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Publisher capacity for a contiguous 16-KiB payload (below the 1 MiB segmentation threshold).",
        qos_publish=0,
        payload="record16k",
        cadence="capacity",
        estimated_minutes=2.0,
    ),
    Scenario(
        name="pub_segment_block_64k",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Publisher capacity for a contiguous 64-KiB payload (below the 1 MiB segmentation threshold).",
        qos_publish=0,
        payload="block64k",
        cadence="capacity",
        estimated_minutes=2.0,
    ),
    Scenario(
        name="pub_segment_block_128k",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Publisher capacity for a 128-KiB payload.",
        qos_publish=0,
        payload="block128k",
        cadence="capacity",
        estimated_minutes=2.0,
    ),
    Scenario(
        name="pub_segment_block_256k",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Publisher capacity for a 256-KiB payload.",
        qos_publish=0,
        payload="block256k",
        cadence="capacity",
        estimated_minutes=2.0,
    ),
    Scenario(
        name="pub_segment_block_512k",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Publisher capacity for a 512-KiB payload.",
        qos_publish=0,
        payload="block512k",
        cadence="capacity",
        estimated_minutes=2.0,
    ),
    Scenario(
        name="pub_segment_blob_1m",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Publisher capacity for a segmented 1-MiB payload.",
        qos_publish=0,
        payload="blob1m",
        cadence="capacity",
        estimated_minutes=2.0,
    ),
    Scenario(
        name="pub_qos1_inflight",
        suite="core",
        tags=("representative", "diagnostic"),
        topology="publisher_only",
        description="QoS 1 capacity across inflight windows.",
        qos_publish=1,
        payload="telemetry256",
        cadence="capacity",
        variants=tuple(
            {"inflight": n, "max_queued": n * 10, "outstanding": max(n, 8)}
            for n in (1, 20, 100)
        ),
        estimated_minutes=4.0,
    ),
    Scenario(
        name="pub_qos1_sendmsg_capacity",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="QoS 1 publish capacity with outbound write-call accounting.",
        qos_publish=1,
        payload="telemetry256",
        cadence="capacity",
        inflight=100,
        max_queued=1000,
        outstanding=100,
        count_socket_writes=True,
        estimated_minutes=3.0,
    ),
    Scenario(
        name="remaining_length_boundaries",
        suite="core",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Exact MQTT Remaining Length byte-width transitions.",
        qos_publish=0,
        cadence="capacity",
        topic_topology="single",
        variants=tuple({"payload": p} for p in ("rl_126", "rl_127", "rl_128", "rl_16383", "rl_16384")),
        estimated_minutes=3.0,
    ),
    Scenario(
        name="sub_exact_telemetry",
        suite="core",
        tags=("representative",),
        topology="subscriber_ingress",
        description="Ingress capacity: multi-publisher emqtt-bench to one exact topic.",
        qos_publish=0,
        qos_subscribe=0,
        payload="telemetry256",
        cadence="capacity",
        topic_topology="single",
        subscription="exact",
        loadgen_clients=32,
        subscribers=1,
        estimated_minutes=3.0,
    ),
    Scenario(
        name="sub_exact_qos1_capacity",
        suite="full",
        tags=("diagnostic",),
        topology="subscriber_ingress",
        description="QoS 1 ingress capacity and outbound PUBACK batching.",
        qos_publish=1,
        qos_subscribe=1,
        payload="telemetry256",
        cadence="capacity",
        topic_topology="single",
        subscription="exact",
        loadgen_clients=32,
        subscribers=1,
        variants=tuple(
            {"target_rate": rate, "count_socket_writes": True}
            for rate in (5000, 10000, 15000, 20000)
        ),
        estimated_minutes=3.0,
    ),
    Scenario(
        name="sub_hierarchy_telemetry",
        suite="core",
        tags=("representative",),
        topology="subscriber_ingress",
        description="Ingress across fleet topics with + and # broker filters.",
        qos_publish=0,
        qos_subscribe=0,
        payload="telemetry256",
        topic_topology="fleet4k_uniform",
        loadgen_clients=32,
        subscribers=1,
        variants=(
            {"topic_topology": "fleet4k_uniform", "subscription": "plus"},
            {"topic_topology": "fleet4k_uniform", "subscription": "hash"},
            {"topic_topology": "fleet4k_zipf", "subscription": "plus"},
            {"topic_topology": "fleet4k_zipf", "subscription": "hash"},
        ),
        estimated_minutes=6.0,
    ),
    Scenario(
        name="sub_callback_matching",
        suite="core",
        tags=("representative", "diagnostic"),
        topology="subscriber_ingress",
        description="Local Paho message_callback_add matching cost.",
        qos_subscribe=0,
        payload="telemetry256",
        topic_topology="fleet4k_uniform",
        subscription="hash",
        loadgen_clients=32,
        subscribers=1,
        variants=tuple({"callback_filters": n} for n in (1, 16, 256)),
        estimated_minutes=5.0,
    ),
    Scenario(
        name="duplex_gateway",
        suite="core",
        tags=("representative",),
        topology="duplex_gateway",
        description="Gateway pair: SUT publishes telemetry while a SUT subscriber receives injected commands.",
        qos_publish=0,
        qos_subscribe=1,
        payload="telemetry256",
        variants=tuple({"cadence": c} for c in ("steady50", "burst")),
        estimated_minutes=4.0,
    ),
    Scenario(
        name="burst_recovery",
        suite="core",
        tags=("representative",),
        topology="subscriber_ingress",
        description="Ingress under bursty load with backlog recovery metrics.",
        qos_subscribe=0,
        payload="telemetry256",
        cadence="burst",
        topic_topology="fleet4k_uniform",
        subscription="hash",
        loadgen_clients=32,
        subscribers=1,
        estimated_minutes=3.0,
    ),
    Scenario(
        name="e2e_integrity",
        suite="core",
        tags=("functional", "representative"),
        topology="publisher_with_oracle",
        description="Sequence integrity for QoS 0/1/2 plus empty payload QoS 0.",
        cadence="steady50",
        subscribers=1,
        variants=(
            {"qos_publish": 0, "qos_subscribe": 0, "payload": "telemetry256", "force_header": True},
            {"qos_publish": 1, "qos_subscribe": 1, "payload": "telemetry256", "force_header": True},
            {"qos_publish": 2, "qos_subscribe": 2, "payload": "telemetry256", "force_header": True},
            {"qos_publish": 0, "qos_subscribe": 0, "payload": "empty0", "force_header": True},
        ),
        estimated_minutes=5.0,
    ),
    Scenario(
        name="puback_latency_qos1",
        suite="core",
        tags=("representative",),
        topology="publisher_only",
        description="Open-loop PUBACK latency at calibrated load fractions.",
        qos_publish=1,
        payload="telemetry256",
        cadence="loaded75",
        variants=tuple({"load_fraction": f} for f in (0.25, 0.50, 0.75, 0.90)),
        estimated_minutes=6.0,
    ),
    Scenario(
        name="rtt_capacity_qos1",
        suite="core",
        tags=("diagnostic",),
        topology="application_rtt",
        description="Closed-loop RTT capacity used to calibrate RTT load fractions.",
        qos_publish=1,
        qos_subscribe=1,
        payload="telemetry256",
        cadence="capacity",
        outstanding=32,
        subscribers=1,
        estimated_minutes=3.0,
    ),
    Scenario(
        name="application_rtt_qos1",
        suite="core",
        tags=("representative",),
        topology="application_rtt",
        description="Application request/response RTT with responder process.",
        qos_publish=1,
        qos_subscribe=1,
        payload="telemetry256",
        cadence="loaded75",
        outstanding=32,
        subscribers=1,
        variants=tuple({"load_fraction": f} for f in (0.25, 0.50, 0.75, 0.90)),
        estimated_minutes=6.0,
    ),
    # --- full suite ---
    Scenario(
        name="payload_stress",
        suite="full",
        tags=("stress",),
        topology="publisher_only",
        description="Large payload and str-encoding stress.",
        variants=(
            {"payload": "blob8m", "qos_publish": 0},
            {"payload": "telemetry256_str", "qos_publish": 0},
            {"payload": "block64k", "qos_publish": 1},
            {"payload": "blob1m", "qos_publish": 1},
        ),
        estimated_minutes=6.0,
    ),
    Scenario(
        name="topic_stress",
        suite="full",
        tags=("stress",),
        topology="subscriber_ingress",
        description="Cardinality, depth, unicode and callback matching stress.",
        loadgen_clients=16,
        subscribers=1,
        variants=(
            {"topic_topology": "fleet100k", "subscription": "hash"},
            {"topic_topology": "deep32", "subscription": "exact"},
            {"topic_topology": "long_topic_256", "subscription": "exact"},
            {"topic_topology": "long_topic_1024", "subscription": "exact"},
            {"topic_topology": "unicode", "subscription": "exact"},
            {"topic_topology": "fleet4k_uniform", "subscription": "hash", "callback_filters": 4096},
            {"topic_topology": "fleet4k_uniform", "subscription": "hash", "callback_filters": 8, "overlapping_callbacks": True},
        ),
        estimated_minutes=10.0,
    ),
    Scenario(
        name="sub_multi_subscribe",
        suite="full",
        tags=("diagnostic",),
        topology="subscriber_ingress",
        description="Many exact MQTT subscriptions on one client.",
        loadgen_clients=16,
        subscribers=1,
        variants=tuple({"subscription_count": n, "subscription": "multi_exact"} for n in (16, 256)),
        estimated_minutes=4.0,
    ),
    Scenario(
        name="fanin_scaling",
        suite="full",
        tags=("stress",),
        topology="subscriber_ingress",
        description="Fan-in scaling with constant aggregate and per-publisher rates.",
        subscribers=1,
        subscription="hash",
        topic_topology="fleet4k_uniform",
        variants=(
            {"loadgen_clients": 1, "fanin_mode": "constant_aggregate"},
            {"loadgen_clients": 16, "fanin_mode": "constant_aggregate"},
            {"loadgen_clients": 128, "fanin_mode": "constant_aggregate"},
            {"loadgen_clients": 1, "fanin_mode": "per_publisher"},
            {"loadgen_clients": 16, "fanin_mode": "per_publisher"},
            {"loadgen_clients": 128, "fanin_mode": "per_publisher"},
        ),
        estimated_minutes=8.0,
    ),
    Scenario(
        name="fanout_scaling",
        suite="full",
        tags=("stress",),
        topology="publisher_with_oracle",
        description="One publisher to many Paho subscribers.",
        qos_publish=0,
        qos_subscribe=0,
        variants=tuple({"subscribers": n} for n in (1, 8, 32)),
        estimated_minutes=6.0,
    ),
    Scenario(
        name="periodic_and_microburst",
        suite="full",
        tags=("diagnostic", "stress"),
        topology="subscriber_ingress",
        description="Low-rate periodic and microburst ingress shapes.",
        loadgen_clients=8,
        subscribers=1,
        variants=(
            {"cadence": "periodic10"},
            {"cadence": "microburst"},
        ),
        estimated_minutes=4.0,
    ),
    Scenario(
        name="mqttv5_properties",
        suite="full",
        tags=("representative", "diagnostic"),
        topology="publisher_with_oracle",
        description="MQTT v3 vs v5 empty vs realistic properties.",
        subscribers=1,
        variants=(
            {"protocol": "MQTTv311", "properties_profile": "none"},
            {"protocol": "MQTTv5", "properties_profile": "none"},
            {"protocol": "MQTTv5", "properties_profile": "realistic"},
        ),
        estimated_minutes=5.0,
    ),
    Scenario(
        name="mqttv5_rich",
        suite="full",
        tags=("stress",),
        topology="publisher_with_oracle",
        description="Heavy MQTT v5 properties, topic alias and subscription identifiers.",
        protocol="MQTTv5",
        subscribers=1,
        variants=(
            {"properties_profile": "rich"},
            {"properties_profile": "topic_alias"},
            {"properties_profile": "subscription_identifier"},
        ),
        estimated_minutes=5.0,
    ),
    Scenario(
        name="mqttv5_flow_control",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_only",
        description="Receive Maximum vs client inflight interaction.",
        protocol="MQTTv5",
        qos_publish=1,
        variants=(
            {"receive_maximum": 10, "inflight": 100},
            {"receive_maximum": 100, "inflight": 100},
        ),
        estimated_minutes=4.0,
    ),
    Scenario(
        name="qos_asymmetric",
        suite="full",
        tags=("diagnostic",),
        topology="publisher_with_oracle",
        description="Asymmetric publish/subscribe QoS pairs.",
        cadence="steady50",
        subscribers=1,
        variants=(
            {"qos_publish": 1, "qos_subscribe": 0},
            {"qos_publish": 2, "qos_subscribe": 1},
            {"qos_publish": 0, "qos_subscribe": 1},
        ),
        estimated_minutes=4.0,
    ),
    Scenario(
        name="queue_rejection",
        suite="full",
        tags=("functional",),
        topology="publisher_only",
        description="Queue rejection accounting under controlled pressure.",
        qos_publish=1,
        inflight=1,
        max_queued=100,
        cadence="capacity",
        variants=({"expected_accepts": 100, "expected_rejects": 50, "submit_count": 150},),
        estimated_minutes=2.0,
    ),
    Scenario(
        name="retained_bootstrap",
        suite="full",
        tags=("stress", "functional"),
        topology="subscriber_ingress",
        description="Retained message bootstrap snapshot (broker-sensitive).",
        subscribers=1,
        variants=tuple({"retained_count": n} for n in (10_000, 100_000)),
        estimated_minutes=6.0,
    ),
    Scenario(
        name="session_resume_qos1",
        suite="full",
        tags=("functional",),
        topology="publisher_with_oracle",
        description="Persistent session resume after short outage.",
        qos_publish=1,
        qos_subscribe=1,
        cadence="steady50",
        subscribers=1,
        variants=({"session_persistent": True, "outage_s": 2.0, "expected_drain": 2000},),
        estimated_minutes=3.0,
    ),
    Scenario(
        name="network_matrix",
        suite="full",
        tags=("diagnostic", "functional"),
        topology="publisher_only",
        description="Network profile matrix for latency, integrity and reconnect.",
        qos_publish=1,
        payload="telemetry256",
        variants=(
            {"network": "localhost", "cadence": "loaded75"},
            {"network": "lan", "cadence": "loaded75"},
            {"network": "wan", "cadence": "loaded75"},
            {"network": "edge", "cadence": "steady50", "integrity": True},
            {"network": "wan_cut", "cadence": "steady50", "session_persistent": True},
        ),
        estimated_minutes=10.0,
    ),
    Scenario(
        name="tls_steady_state",
        suite="full",
        tags=("representative",),
        topology="publisher_only",
        description="Steady-state publish capacity over established TLS 1.3.",
        qos_publish=1,
        payload="telemetry256",
        tls=True,
        estimated_minutes=3.0,
    ),
    Scenario(
        name="connect_latency_and_churn",
        suite="full",
        tags=("diagnostic", "stress"),
        topology="connect",
        description="TCP/TLS connect latency and connection storms.",
        variants=(
            {"connect_mode": "tcp_serial", "connect_count": 100},
            {"connect_mode": "tls_serial", "connect_count": 100, "tls": True},
            {"connect_mode": "tls_resume", "connect_count": 100, "tls": True},
            {"connect_mode": "tcp_concurrent", "connect_count": 32},
            {"connect_mode": "tcp_concurrent", "connect_count": 256},
        ),
        estimated_minutes=6.0,
    ),
    Scenario(
        name="client_fleet_idle",
        suite="full",
        tags=("diagnostic",),
        topology="fleet",
        description="Idle fleet keepalive/RSS/CPU cost.",
        variants=tuple({"fleet_size": n, "keepalive": 30} for n in (1, 32, 256)),
        estimated_minutes=5.0,
    ),
]


SCENARIO_BY_NAME = {s.name: s for s in SCENARIOS}


def list_scenarios(suite: Optional[str] = None) -> List[Scenario]:
    if suite is None:
        return list(SCENARIOS)
    return [s for s in SCENARIOS if s.suite == suite]


def expand_scenario(scenario: Scenario, profile: str = "standard") -> List[Dict[str, Any]]:
    """Expand a scenario into concrete run points with profile-adjusted timings."""
    base_variants = scenario.variants or ({},)
    points = []
    for variant in base_variants:
        resolved = scenario.resolved(variant)
        if profile == "smoke":
            resolved["duration_s"] = min(float(resolved.get("duration_s", 60.0)), 3.0)
            resolved["warmup_s"] = min(float(resolved.get("warmup_s", 15.0)), 1.0)
            resolved["drain_s"] = min(float(resolved.get("drain_s", 30.0)), 2.0)
            resolved["non_comparable"] = True
        else:
            resolved["non_comparable"] = False
        resolved["scenario"] = scenario.name
        resolved["suite"] = scenario.suite
        resolved["tags"] = list(scenario.tags)
        resolved["description"] = scenario.description
        points.append(resolved)
    return points


def estimate_suite(suite: str, profile: str, runs: int) -> dict:
    scenarios = list_scenarios(suite)
    points = sum(len(expand_scenario(s, profile)) for s in scenarios)
    minutes = sum(s.estimated_minutes for s in scenarios)
    if profile == "smoke":
        minutes = minutes * 0.05
        runs = 1
    else:
        minutes = minutes * (runs / 7.0)
    return {
        "suite": suite,
        "scenarios": len(scenarios),
        "points": points,
        "runs_per_point": runs,
        "estimated_minutes": round(minutes, 1),
    }
