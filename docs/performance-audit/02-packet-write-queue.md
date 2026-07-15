# 02 - Packet Write Queue

## Problem

The send path is a probable P0 bottleneck for producers and gateways publishing
large message volumes. The hot code paths are `Client.publish()`,
`Client._send_publish()`, `Client._packet_queue()`, and `Client._packet_write()`.

Likely symptoms:

- High CPU use during QoS 0 publish bursts.
- Frequent packet allocation and payload copying.
- Repeated socketpair wakeups when many publishes arrive before the network
  thread drains the queue.
- Repeated `want_write()` checks and socket register/unregister callbacks.
- `MQTTMessageInfo` and condition objects created for every QoS 0 publish even
  when callers do not wait on them.

Common workloads:

- IoT aggregators forwarding many telemetry messages to a broker.
- Producer clients using `loop_start()` and publishing from another thread.
- QoS 0 firehose workloads where the Python client is CPU-bound.
- QoS 1 workloads with high inflight limits and small payloads.

## Theoretical Rationale

Outbound publishing performs several CPU-visible operations per message:

- Topic encoding and validation.
- Payload normalization.
- Remaining length encoding.
- `bytearray` construction and extension.
- Queue object creation.
- Possible immediate `loop_write()` call.
- Socketpair byte write to wake the network loop.

For small messages, fixed per-message overhead dominates payload transmission.
Repeated wakeups can also cause unnecessary context switches, cache disruption,
and syscall overhead. Batching queue drain and coalescing wakeups are often more
valuable than optimizing individual arithmetic operations.

## Expected Gain

Priority: P0.

Conservative expected gain:

- 10 to 30 percent throughput improvement for QoS 0 publish bursts from a worker
  thread into `loop_start()`.
- 5 to 15 percent CPU reduction for single-threaded publish plus loop workloads.
- Reduced system CPU when socketpair wakeups are coalesced.

The largest gain should appear when messages are small and publish rate is much
higher than the network loop wakeup rate.

## Before/After Measurements

Microbenchmarks:

- Call `_send_publish()` with pre-encoded topic and payload against a fake socket
  that accepts all writes.
- Call `publish()` for QoS 0 with 16-byte and 128-byte payloads.
- Call `publish()` for QoS 1 with inflight available and with inflight saturated.
- Measure `_packet_write()` draining 1, 10, 100, and 10,000 queued packets.
- Count socketpair writes per 10,000 queued packets in threaded mode.

Broker scenarios:

- Local TCP QoS 0 publish throughput with `loop_start()`.
- Local TCP QoS 0 publish throughput with manual `loop()`.
- Local TCP QoS 1 publish throughput with inflight limits 20, 100, and 1000.
- TLS publish throughput for 128-byte and 4-KiB payloads.

Metrics:

- Published messages per second.
- CPU user/system split.
- Socketpair wakeups per message.
- Queue length high-water mark.
- p95 time from `publish()` call to socket send where measurable.

## Implementation Guidelines

Allowed implementation directions:

- Add an internal wakeup-pending flag so `_packet_queue()` writes to the
  socketpair only when the network loop has not already been woken.
- Batch `_packet_write()` drain behavior so one readiness event can send more
  queued packets without repeated register/unregister churn.
- Cache `struct.Struct("!H")` or use direct byte construction for common packet
  fields.
- Fast-path MQTT v3 QoS 0 PUBLISH with empty properties and already-bytes
  payloads.
- Avoid avoidable conversions when topic and payload are already bytes.
- Evaluate whether `MQTTMessageInfo` can lazily allocate its condition while
  preserving public behavior.

Risks:

- Wakeup coalescing must not delay messages indefinitely.
- External event loop callbacks must still receive correct register/unregister
  notifications.
- `wait_for_publish()` behavior and callback ordering must remain unchanged.
- QoS 1 and QoS 2 state transitions must remain correct under partial writes.

## Acceptance Criteria

Functional criteria:

- Existing tests pass.
- Add tests for threaded publish wakeup coalescing, external loop write
  registration, partial socket writes, and QoS 0 on_publish ordering.
- Preserve return values and `MQTTMessageInfo` behavior.

Performance criteria:

- At least 10 percent throughput improvement or 10 percent CPU reduction in the
  threaded QoS 0 small-payload benchmark.
- At least 50 percent fewer socketpair writes per publish burst when the network
  loop is already awake.
- No regression above 2 percent in single-message latency for non-threaded
  publish.

Documentation criteria:

- Record wakeup counts before and after.
- Document whether gains come from fewer syscalls, fewer allocations, or faster
  packet construction.

## Verdict

GO with conditions.

Justification: send-side CPU and wakeup overhead directly affect high-volume IoT
publishers. Proceed first with measurement of socketpair wakeups and queue drain
behavior, because batching bugs can create subtle latency regressions.

## Progress (2026-07-09)

Status: **GO — project complete for this round**.

Commits: `6f6869c`, `65e1671`, `6bb33c5` (plus write scenarios from the harness era).

### Implemented (accepted)

1. Shared `_PACK_U16 = struct.Struct("!H")` for topic length / mid packing.
2. Drop dead `remaining_bytes` list in `_pack_remaining_length()`.
3. `_send_publish()` packs already-bytes topics without `_force_bytes()`.
4. Lazy `threading.Condition` on `MQTTMessageInfo`. Hot-path
   `_set_as_published()` is lock-free when no waiter exists; waiters create the
   Condition under `_message_info_condition_lock` and re-check `_published`
   under the Condition. Covered by a concurrent race test.
5. Socketpair wakeup coalescing via `_sockpair_wakeup_pending`, guarded by
   `_sockpair_wakeup_mutex` shared by `_packet_queue()` send and `_loop()` drain.
6. `loop_start()` installs the new sockpair under that mutex and re-wakes if
   packets were queued during the swap (missed-wakeup fix).
7. Avoid full-buffer slice copy in `_packet_write()` when `pos == 0`.
8. Safe `info is None` handling in QoS 0 completion path.
9. Fast-path `_pack_remaining_length` for `RL < 128` while preserving the
   upstream `ValueError("Packet too large")` contract for `RL > 2^28-1`.
10. Harness: `sockpair_wakeup_coalesce_10000`, `publish_threaded_qos0_v3_small`.
11. Tests in `tests/test_client_write_performance.py`.

### Acceptance criteria

| Criterion | Result |
| --- | --- |
| Threaded QoS 0 small-payload ≥ +10% | **PASS** — about +51% vs pre-write HEAD |
| ≥ 50% fewer sockpair writes when loop already awake | **PASS** — 10000 → 1 in coalesce scenario |
| No >2% single-message latency regression (non-threaded) | **PASS** — p50 improved |
| Functional tests for coalesce / partial write / on_publish / external loop | **PASS** |

Representative brokerless deltas (small IoT payloads):

| Scenario | Delta |
| --- | --- |
| `publish_pack_qos0_v3_small` | about +55% to +77% vs early baseline |
| `publish_pack_qos1_v3_small` | about +8% vs early baseline after RL fast-path (upstream size check kept) |
| `packet_write_drain_100` | about +140% to +200% |
| threaded QoS 0 | about +51% |
| sockpair wakeups / burst | −99%+ |

### Evaluated and rejected / deferred

| Track | Verdict | Evidence |
| --- | --- | --- |
| `_OutPacket` `__slots__` class with dict-compatible `__getitem__` | **NO GO** | E2E with attribute shim ~−15%. |
| Skip allocating `MQTTMessageInfo` for fire-and-forget QoS 0 | **NO GO for now** | Public API must return `MQTTMessageInfo`. |
| Preallocate PUBLISH `bytearray` + `pack_into` | **NO GO** | QoS 0 regressed vs simple extend packing. |
| Remove upstream remaining-length size check | **NO GO** | Required by synced #901; use `<128` fast-path instead. |
