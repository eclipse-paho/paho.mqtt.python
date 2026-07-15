# 14 - Contiguous Ingress Decoder

## Analysis

Project 09 removed most kernel reads from small inbound bursts, but the packet
parser still consumes the private read-ahead buffer through separate logical
reads for the command byte, Remaining Length, and packet body. Each completed
packet therefore crosses `_sock_recv_read_ahead()` several times and creates
small slices even when all bytes are already contiguous in memory.

The built-in loop can instead parse MQTT framing directly from a bounded byte
arena. A cursor can locate the command, decode the one-to-four-byte Remaining
Length, verify that the full packet is present, and dispatch a view of its body.
Only data that must outlive dispatch, notably `MQTTMessage.payload`, needs an
owned copy.

The public `loop_read(max_packets)` path has a separate issue: its argument is
currently overwritten by the number of tracked QoS messages. A caller asking
for a larger batch therefore still processes one packet in a QoS 0 workload.
This existing argument can be honored without enabling read-ahead that could
strand prefetched bytes outside the external event loop's readiness model.

## Preparation

- Preserve the current packet-at-a-time parser as the reference implementation
  during paired measurements.
- Add a brokerless contiguous-stream source that exposes 1, 10, 100, and 1,000
  MQTT v3 and v5 PUBLISH packets in one receive buffer.
- Add local socketpair TCP-equivalent and TLS scenarios with partial reads of
  1, 2, 7, 128, and 65,536 bytes.
- Measure `loop_read(1)`, `loop_read(100)`, and the private built-in batch path
  separately.
- Record parser calls, buffer slices/copies, `recv()` calls, CPU, allocations,
  throughput, p50, and p95 latency.
- Keep WebSocket framing out of this project; project 21 owns that transport.

## Expected Gain

Priority: P0.

- 15 to 30 percent higher small-PUBLISH throughput after project 09.
- Lower allocation and function-call counts per packet.
- At least 20 percent higher external-loop burst throughput when callers use
  the already-existing `max_packets` argument.
- Neutral behavior for isolated packets and large payloads.

## Acceptance Criteria

- At least 15 percent higher throughput for 1,000 small TCP/Unix PUBLISH
  packets in the built-in loop.
- At least 10 percent higher throughput for the equivalent TLS burst.
- At least 20 percent higher throughput and 80 percent fewer readiness
  callbacks for `loop_read(100)` versus `loop_read(1)`.
- No regression above 2 percent for 64-KiB payloads.
- No p95 latency regression above 5 percent for one isolated message.
- The default `loop_read(1)` preserves the current effective QoS behavior; the
  effective limit is `max(requested, tracked QoS work, 1)`.
- Remaining Length overflow, non-minimal/truncated encodings, partial packets,
  reconnect buffer reset, TLS `pending()`, and the 100-packet fairness ceiling
  remain correct.
- Retained arena capacity is bounded by the largest active packet plus a
  64-KiB read window, and historical traffic cannot grow it indefinitely.

## Before Measurement

Environment: CPython 3.12.3, Linux x86-64. The final decoder comparison uses 2
warmups and 15 paired runs in alternating legacy/candidate order in the same
process. Guard scenarios use 15 harness runs.

Baseline results:

| Scenario | Throughput | p95 | Allocations | `recv` / parser calls |
| --- | ---: | ---: | ---: | ---: |
| Built-in plain burst, 10,000 small v3 PUBLISH | 94,997 msg/s | paired range recorded by eval | 406,894 B peak at 3,000 | 11 `recv`, 100 batch calls |
| Built-in TLS burst, 3,000 small v3 PUBLISH | 88,480 msg/s | paired range recorded by eval | not isolated | 13 `recv`, 37 batch calls |
| Public legacy QoS 0 loop | 76,435 msg/s | paired range recorded by eval | not isolated | 30,000 `recv`, 10,000 loop calls |
| Packet-at-a-time small PUBLISH | 88,569 msg/s | 12.15 us | not traced | 3 logical reads/message |
| Packet-at-a-time 64-KiB PUBLISH | 7,650 msg/s | 157.32 us | not traced | regression guard |

## Implementation

Implemented prototype:

- Reuse project 09's bounded immutable read-ahead buffer and cursor rather than
  adding a second arena.
- Decode command and Remaining Length directly from contiguous buffered bytes.
- Dispatch a short-lived `memoryview` of a complete packet body and restore the
  reusable incremental `bytearray` afterward.
- Fall back to the existing incremental parser whenever the fixed header or
  packet body crosses a receive-buffer boundary.
- Keep payload ownership, public `MQTTMessage`, WebSocket behavior, and all
  close/reconnect buffer reset paths unchanged.
- Add focused tests for view dispatch, partial fallback, invalid Remaining
  Length, fairness, and reusable-state restoration.
- Add `benchmarks/ingress_decoder_eval.py` for alternating same-process plain,
  TLS, public-loop, and `tracemalloc` comparisons.
- A prototype honoring `loop_read(100)` for QoS 0 reduced loop calls from
  10,000 to 100 but missed its throughput threshold; that production change
  and its behavior test were removed. Its standalone measurement remains in
  the evaluation script.

## After Measurements

| Scenario | Before | After | Delta |
| --- | ---: | ---: | ---: |
| Built-in plain, 10,000 small PUBLISH | 94,997 msg/s | 130,363 msg/s | **+37.2%** |
| Built-in TLS, 3,000 small PUBLISH | 88,480 msg/s | 120,739 msg/s | **+36.5%** |
| Public `loop_read(100)` prototype | 76,435 msg/s | 82,686 msg/s | +8.2%, removed |
| Packet-at-a-time small PUBLISH | 88,569 msg/s | 91,972 msg/s | +3.8% |
| Packet-at-a-time 64-KiB PUBLISH | 7,650 msg/s | 8,213 msg/s | +7.4% |
| Plain 3,000-message `tracemalloc` peak | 406,894 B | 406,894 B | 0.0% |

Plain and TLS paired runs use identical receive counts before and after. The
gain therefore isolates Python framing/dispatch overhead rather than claiming
another syscall reduction already delivered by project 09.

## Results Analysis

Direct framing crosses the P0 threshold in both plain and TLS streams. The
packet body no longer passes through three logical read-ahead calls and is not
copied into the reusable incremental buffer before dispatch. Memory stays
neutral because the candidate retains the same bounded 64-KiB read window.

The partial-packet path deliberately remains incremental and all focused plus
socket-backed client/MQTT v5 tests pass. Packet-at-a-time and 64-KiB guards
improve, so the extra reusable-buffer identity check does not penalize public
reads.

The external-loop subtrack does reduce readiness-level calls by about 99
percent, but its paired throughput gain is only 8.2 percent versus the required
20 percent. It is therefore retained only as benchmark evidence. No read-ahead
or changed `loop_read()` behavior remains in production.

## Verdict

**GO with conditions.** Keep the contiguous built-in decoder and its
partial-parser fallback. Reject and remove the public `loop_read(max_packets)`
behavior change because it missed its independent threshold.
