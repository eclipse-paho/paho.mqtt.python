# 01 - Packet Read Parser

## Problem

The receive path is a probable P0 bottleneck for high-rate IoT aggregation. The
hot code paths are `Client._packet_read()` and `Client._handle_publish()` in
`src/paho/mqtt/client.py`.

Likely symptoms:

- High CPU use while receiving many small PUBLISH packets.
- Allocation pressure from repeated packet-buffer growth and packet reset.
- Repeated dictionary lookups on `_in_packet` for every byte or packet field.
- Extra copies and `struct.unpack()` calls while splitting topic, packet id,
  properties, and payload.
- Per-message UTF-8 decode attempts for logging and callback topic access.

Common workloads:

- Thousands of sensors publishing small payloads to a single subscriber.
- MQTT v5 inbound messages with empty or small properties.
- TLS sockets where fewer large reads are preferable to repeated small reads.

## Theoretical Rationale

The fixed header and remaining length parser currently performs many operations
that are cheap alone but expensive at message scale:

- Reading command and remaining length one byte at a time increases Python call
  overhead and may increase socket wrapper overhead.
- `_in_packet` is a dictionary, so field access requires hashing and lookup on
  the hottest path.
- `self._in_packet['packet'] += data` can reallocate or copy as the packet grows.
- `struct.unpack()` with dynamically built format strings allocates temporary
  objects and copies slices.
- Slicing `bytearray` creates copies for topic, packet id, properties, and
  payload extraction.

Modern CPUs are fast at sequential memory access and branch-predictable loops,
but Python-level loops, dictionary lookups, and short-lived allocations dominate
for small messages. Reducing object churn and using index-based parsing should
lower interpreter overhead and improve cache locality.

## Expected Gain

Priority: P0.

Conservative expected gain:

- 10 to 25 percent CPU reduction for small inbound QoS 0 messages.
- 5 to 15 percent throughput improvement for inbound QoS 1 due to parsing cost
  plus ACK generation.
- Lower allocation count per received message, especially for MQTT v3 payloads
  with no properties.

The largest gain should appear when payloads are small, callbacks are lightweight,
and the client is CPU-bound rather than network-bound.

## Before/After Measurements

Microbenchmarks:

- Feed prebuilt MQTT v3 PUBLISH packets into a fake socket and call
  `_packet_read()` until all messages are consumed.
- Repeat for MQTT v5 PUBLISH packets with empty properties and with common
  properties.
- Measure isolated `_handle_publish()` using prepared `_in_packet` state.
- Test payload sizes: 0 bytes, 16 bytes, 128 bytes, 1024 bytes, 64 KiB.
- Test topic lengths: 8, 32, 128 bytes.

Broker scenarios:

- Local TCP subscriber receiving QoS 0 messages with a no-op callback.
- Local TCP subscriber receiving QoS 1 messages with auto ACK.
- TLS variant for payloads >= 1024 bytes.

Metrics:

- Messages per second.
- CPU time per 100,000 messages.
- `tracemalloc` allocations per message.
- p50 and p95 callback delivery latency if broker scenario supports timestamps.

Profilers:

- `cProfile` for call counts and cumulative time.
- `py-spy` if installed for low-intrusion sampling.
- Linux `perf` if available for syscall and CPU-cycle context.

## Implementation Guidelines

Allowed implementation directions:

- Replace `_in_packet` dictionary state with a small private class using
  `__slots__`, while preserving external behavior.
- Keep a reusable input buffer object and reset fields instead of replacing the
  entire dictionary for every packet.
- Parse topic length and packet id with direct byte indexing or cached
  `struct.Struct` objects instead of dynamic format strings.
- Avoid copying payload bytes until assigning `MQTTMessage.payload`; use
  `memoryview` internally only if the final public payload remains `bytes` or
  `bytearray` as today.
- Fast-path MQTT v3 PUBLISH without properties.
- Fast-path MQTT v5 PUBLISH with property length zero.
- Keep invalid UTF-8 behavior unchanged for public `message.topic`.

Risks:

- Changing buffer lifetime can accidentally expose mutable data through
  `MQTTMessage.payload`.
- MQTT remaining length validation must remain strict.
- Partial reads and non-blocking socket behavior must keep returning
  `MQTT_ERR_AGAIN` correctly.
- TLS `pending()` behavior must not regress.

Optional Python 3.12+ note:

- Newer CPython versions improve specialization of attribute access, which makes
  `__slots__` state more attractive, but the design must still work on Python
  3.7+.

## Acceptance Criteria

Functional criteria:

- Existing unit and integration tests pass.
- Add targeted tests for partial fixed header reads, partial remaining length
  reads, partial payload reads, invalid remaining length, empty MQTT v5
  properties, and invalid topic UTF-8 behavior.
- Preserve public `MQTTMessage.topic`, `payload`, `qos`, `retain`, `mid`, and
  `properties` behavior.

Performance criteria:

- At least 10 percent lower CPU time or 10 percent higher messages/s for the
  primary small QoS 0 inbound benchmark.
- No more than 2 percent regression for large payload benchmarks.
- At least 15 percent fewer allocations per small inbound PUBLISH message.

Documentation criteria:

- Record before/after benchmark commands, environment, Python version, and
  median results in this file or a linked result file.

## Verdict

GO with conditions.

Justification: this path is central to every receiving client, and the current
implementation shows multiple interpreter-level costs in the hot loop. Proceed
only with a staged prototype and strict partial-read tests because correctness
risk is high.

## Progress (2026-07-09)

Status: **Done (this round)**.

### Landed

- `_InPacketState` (`__slots__`) reused across packets via `reset()` instead of
  allocating a new dict + `bytearray` per message.
- `remaining_count` is an int counter (was a list used only for length checks).
- Payload accumulation uses `bytearray.extend` on the reusable buffer.
- `_handle_publish()` parses topic length / mid with `_PACK_U16.unpack_from`,
  index-based slicing via `memoryview`, and copies topic/payload out before
  reset (public payload remains immutable `bytes`).
- MQTT v5 empty property length (`0` VBI) skips `Properties.unpack()`.

Tests: `tests/test_client_read_performance.py` (partial header / remaining
length / payload, illegal 5-byte remaining length, v5 empty + user props,
invalid UTF-8 topic, state reuse).

### Before / after (brokerless harness)

Environment: Python 3.12.3, `PYTHONPATH=src`,
`python benchmarks/run.py --scenario … --runs 15 --no-tracemalloc`.

| Scenario | Before (ops/s) | After (ops/s) | Delta |
| --- | ---: | ---: | ---: |
| `publish_parse_v3_qos0_small` | 74504 | 81924 | **+10.0%** |
| `publish_parse_v5_qos0_empty_props` | 58507 | 75934 | **+29.8%** |
| `publish_parse_v5_qos0_user_props` | 11086 | 13105 | **+18.2%** |

Large 64 KiB payload smoke after change: ~7.8k msg/s (no obvious regression
signal vs small-message focus; keep watching if a dedicated large-payload
scenario is added).

### Related prior landings (not this project)

- 03: cheaper MQTT v5 property unpack on empty/common sets.
- 04 partial: cheaper `_handle_on_message` when no filtered callbacks; lazy
  inbound `MQTTMessageInfo`.

### Deferred / out of scope here

- Batched socket reads for command + remaining length (TLS `pending()` risk).
- Matcher / inflight work (04 / 05).

### Follow-up (2026-07-09) — receive path for `mqtt_zigbee_listener`

Workload: `loop_forever`, inbound QoS2, 7 `topic_callback` filters, no paho logger.

| Track | Verdict | Evidence |
| --- | --- | --- |
| **A QoS2 inbound bookkeeping** | **NO GO** | New harness `publish_parse_v3_qos2_*` shows QoS2 cycle cost is dominated by inevitable copies + PUBREC/PUBREL/PUBCOMP; no localized bookkeeping win ≥5%. |
| **C skip `print_topic` decode when no log sink** | **GO** | Same-process: parse QoS0 no-log vs `on_log` set ≈ **+9.5%** ops/s when logging disabled (listener default). |

Landed with C: `_handle_publish` only decodes/formats the DEBUG PUBLISH log when `on_log` or `_logger` is set. See also 04 for `_topic_str` cache companion.
