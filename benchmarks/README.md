# Paho MQTT Python Benchmarks

This directory contains a standalone, standard-library-only benchmark harness
for profiling implementation hot paths from a source checkout. It is not wired
into pytest, tox, or CI by default.

The first version is intentionally brokerless. It focuses on CPU-heavy paths
that are useful before optimizing packet parsing, packet writing, MQTT v5
properties, reason codes, callback matching, and logging.

For end-to-end **client** benchmarks against a real broker (throughput, latency,
integrity, A/B comparisons), see [`benchmarks/client/`](client/).

## Quick Start

List scenarios:

```bash
python benchmarks/run.py --list
```

Run one scenario:

```bash
python benchmarks/run.py --scenario properties_pack_empty --runs 7
```

Run the paired same-process project 14 evaluation:

```bash
PYTHONPATH=src python benchmarks/ingress_decoder_eval.py
```

Run the project 18 construction/allocation and local-socket evaluations:

```bash
PYTHONPATH=src:benchmarks python benchmarks/segmented_payload_eval.py
PYTHONPATH=src python benchmarks/segmented_payload_socket_eval.py
```

Run the rejected project 19 starvation control:

```bash
PYTHONPATH=src:benchmarks python benchmarks/duplex_scheduler_eval.py
```

Save a baseline and a candidate:

```bash
python benchmarks/run.py --scenario all --runs 7 --output baseline.json
python benchmarks/run.py --scenario all --runs 7 --output candidate.json
```

Compare two result files:

```bash
python benchmarks/compare.py baseline.json candidate.json
```

## Methodology

Each scenario has deterministic inputs, a warmup phase, measured runs, and a
fixed operation unit such as `message`, `packet`, `property-set`, or `log-call`.
The runner disables cyclic GC during each timed run and restores it afterward.

By default, `tracemalloc` is enabled and reports net allocation deltas plus peak
traced memory during each run. Use `--no-tracemalloc` when measuring wall-clock
time with less instrumentation overhead.

Recommended workflow:

1. Run at least 5 measured runs.
2. Keep the same Python, machine, and checkout state for baseline and candidate.
3. Compare medians rather than single runs.
4. Treat small deltas as noise until they reproduce.

## Scenarios

MQTT v5 codec:

- `properties_pack_empty`
- `properties_unpack_empty`
- `properties_pack_common`
- `properties_unpack_common`
- `properties_pack_user_properties`
- `properties_unpack_user_properties`
- `reasoncode_create_puback_success`

Packet read/parser:

- `publish_parse_v3_qos0_small`
- `publish_parse_v5_qos0_empty_props`
- `publish_parse_v5_qos0_user_props`
- `publish_parse_v3_qos2_small` — inbound QoS2 PUBLISH+PUBREL cycle
- `publish_parse_v3_qos2_z2m_filters` — QoS2 + 7 Z2M `topic_callback` filters
- `publish_parse_v3_qos0_large` — 64-KiB regression guardrail
- `loop_read_batch_v3_qos0_small` — bounded read-ahead/batch drain
- `loop_read_public_v3_qos0_small` — public `loop_read(100)` QoS 0 burst

Packet write/queue:

- `publish_pack_qos0_v3_small`
- `publish_pack_qos1_v3_small`
- `packet_write_drain_100`
- `packet_write_drain_10000`

Supporting signals:

- `matcher_many_filters`
- `dispatch_no_filters` / `dispatch_one_filter` / `dispatch_many_filters`
- `dispatch_z2m_seven_filters` — filters from `mqtt_zigbee_listener`
- `logging_disabled`
- `puback_qos1_no_callback`
- `puback_batch_refill_qos1` — 100-ACK saturated refill batch
- `reconnect_reset_qos2_1000`
- `websocket_frame_16` / `websocket_frame_128` / `websocket_frame_1024`

## Result Files

`run.py` emits JSON with:

- schema version
- environment metadata
- scenario parameters
- per-run measurements
- median/min/max wall time
- p50/p95 time per operation
- operations per second
- CPU timing when available
- allocation deltas when enabled

Result files are local artifacts. Do not commit generated baseline/candidate
JSON files unless they are intentionally curated for documentation.

## Comparison Verdicts

`compare.py` reports one verdict per matching scenario:

- `gain`: candidate is at least 5 percent faster.
- `regression`: candidate is at least 3 percent slower.
- `noise`: candidate is between those thresholds.
- `missing`: scenario exists in only one result file.

Allocation deltas are reported separately and are not folded into the speed
verdict.
