# Paho MQTT Client Benchmarks

End-to-end client benchmark harness for measuring **Paho MQTT Python** under
realistic publish/subscribe workloads. This suite is independent from the
brokerless microbenchmarks in `benchmarks/`.

Paho is always the system under test. Mosquitto provides a local broker on
`127.0.0.1:11883` (TCP) and `127.0.0.1:11884` (TLS) to avoid colliding with a
system broker on 1883/8883. `emqtt-bench` is used only as an ingress load
generator.

## Quick start

```bash
# Generate TLS certs and start Mosquitto (Docker required)
python benchmarks/client/run.py broker up

# List scenarios
python benchmarks/client/run.py list --suite core

# Smoke run (short, non-comparable)
python benchmarks/client/run.py run --scenario pub_qos_sweep_telemetry --profile smoke

# Publisher payload sweep
python benchmarks/client/run.py run --scenario pub_payload_sweep_qos0 --profile smoke --output /tmp/pub.json

# Ingress (needs emqtt-bench image)
python benchmarks/client/run.py run --scenario sub_exact_telemetry --profile smoke

# Stop broker
python benchmarks/client/run.py broker down
```

## Commands

| Command | Purpose |
|---|---|
| `broker up` / `broker down` | Local Mosquitto via docker compose (`network_mode: host`) |
| `list [--suite core\|full]` | Scenario catalogue |
| `run --scenario NAME` | Run one scenario |
| `run --suite core\|full` | Run a suite |
| `calibrate --source PATH --output load.json` | Baseline capacity to open-loop fractions |
| `compare --baseline-source A --candidate-source B --scenario NAME` | ABBA A/B comparison |

Useful flags:

- `--profile smoke|standard` — smoke is short and marked `non_comparable`; standard is the comparable profile (7 runs, long windows).
- `--source` — Paho source root containing `src/paho` (for A/B worktrees).
- `--broker host:port` — external broker (`managed_broker=false`).
- `--network localhost|lan|wan|edge|wan_cut` — network profile (emulated profiles need `tc` + `CAP_NET_ADMIN`).
- `--load-profile` — JSON produced by `calibrate`.
- `--output` — write full JSON result.

## What is measured

Three protocols are never mixed:

1. **Capacity** — closed-loop bounded outstanding window; primary metric is completions in `[T0,T1)`.
2. **Latency** — open-loop at calibrated fractions of baseline capacity; percentiles are conditional on success and accompanied by failure rate.
3. **Integrity** — bounded-rate sequence checks (missing/duplicate/out-of-order).

Metric meanings:

| Metric | Meaning |
|---|---|
| `publish_calls` | API `publish()` attempts |
| `publish_accepted` | API accepted the message |
| `socket_completed_qos0` | QoS0 left the client socket (`on_publish`) |
| `protocol_completed` | QoS1 PUBACK / QoS2 PUBCOMP success |
| `subscriber_delivered` | Application deliveries (`on_message` or matched `message_callback_add` callbacks) |
| `callbacks_per_s` | Callback invocations per second (> `msgs_per_s` with overlapping filters) |
| `completed_in_window` | Completions inside the measured window only |
| `completed_during_drain` | Completions after T1 (not counted in rate) |
| `application_rtt` | Request/response with responder process (not one-way timestamp) |

## Workload axes

The suite uses a telemetry anchor workload and varies one axis at a time:

- payloads: empty to 1 MiB (8 MiB in full)
- topics: single, fleet 4k uniform/Zipf, wildcards, local callback matching
- cadences: capacity, steady/loaded fractions, batch, burst
- topologies: publisher-only, ingress via emqtt-bench, fan-in/out, duplex, RTT
- MQTT state: QoS, inflight, v5 properties, retained bootstrap, session resume
- network: localhost (default A/B), LAN/WAN/edge/`wan_cut`

See `scenarios.py` for the full catalogue (`core` and `full`).

## Validity

A run is `valid` only if barriers succeed, no unexpected disconnects occur, and
loadgen/broker are not saturated. Otherwise the run is kept as `inconclusive`
with explicit reasons. Do not treat inconclusive rates as Paho scores.

Additional A/B safeguards:

- Managed-broker runs resolve and observe the real compose container name and
  fail closed if another checkout already owns ports 11883/11884.
- Ingress capacity offers up to 40k msg/s in smoke as well as standard; a 5k
  offer was insufficient to expose receive-path differences.
- RTT load fractions are calibrated from closed-loop RTT capacity, independently
  from publisher capacity, and both RTT endpoints use `TCP_NODELAY`.
- Every result records the resolved Paho source module and declared version.
- Dedicated 16-KiB, 64-KiB, and 1-MiB scenarios isolate segmented-payload
  thresholds instead of relying on one shuffled sweep point.

## Known limitations (not yet implemented)

Points using the following knobs are refused with `not_implemented:*` reasons
instead of silently measuring something else:

- `receive_maximum` (MQTT v5 flow control interaction)
- `retained_count` (retained bootstrap requires pre-seeding the broker)
- `outage_s` / session resume (controlled outage orchestration)
- `submit_count` (queue-rejection accounting protocol)
- `properties_profile` `topic_alias` / `subscription_identifier`
- `connect_mode` `tls_resume` / `tcp_concurrent` (churn probe is serial)
- `topic_topology` `fleet4k_zipf` / `fleet100k` (loadgen publishes one fixed topic,
  so cardinality/skew is not actually offered)

`burst` / `microburst` ingress cadences are implemented as a bounded burst via
emqtt-bench `-L` (global message cap) launched inside the measure window;
recovery shows up in `delivered_during_drain`.

## A/B comparison

```bash
python benchmarks/client/run.py calibrate --source ../paho-baseline --output /tmp/load.json
python benchmarks/client/run.py compare \
  --baseline-source ../paho-baseline \
  --candidate-source . \
  --scenario pub_qos_sweep_telemetry \
  --point-index 1 \
  --blocks 4 \
  --load-profile /tmp/load.json \
  --output /tmp/ab.json
```

ABBA ordering reduces thermal/order bias. Verdict requires a bootstrap CI that
excludes zero **and** an absolute effect above 3%.
`--point-index` selects a resolved scenario variant (zero-based); it defaults
to the first variant for compatibility.

## Reproducibility checklist

- Idle machine; note load average.
- Same Python, same machine, same broker image/digest.
- Use `--profile standard` for comparable results.
- Prefer `--source` worktrees over editing in place mid-run.
- Keep network at `localhost` for capacity verdicts.
- Record JSON outputs; do not average percentiles across runs.

## Tests

```bash
python benchmarks/client/tests/test_unit.py
```

## Layout

- `run.py` — CLI
- `harness.py` — orchestration / barriers / drain
- `scenarios.py` — catalogue
- `roles/` — publisher, subscriber, responder, RTT initiator processes
- `broker.py` / `docker-compose.yml` / `mosquitto/` — local broker
- `loadgen.py` — emqtt-bench wrapper
- `network.py` — netem profiles
- `result.schema.json` — result schema
