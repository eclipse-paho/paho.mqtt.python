# 09 - Read-Ahead and Packet Batching

## Analysis

The built-in select loop processed one inbound QoS 0 packet per readiness event, while `_packet_read()` issued separate reads for the command, remaining length, and body. Small-message subscribers therefore paid one `select()` and about three `recv()` calls per message.

## Preparation

The evaluation uses 1,000 preloaded MQTT v3 QoS 0 PUBLISH packets over a local non-blocking socketpair, plus brokerless small and 64-KiB parser scenarios. The public `loop_read()` path remains packet-oriented; batching is private to the built-in loop.

## Expected Gain

Priority: P0. Target at least 20% higher burst throughput and 60% fewer socket reads, without harming isolated-message latency or large payloads.

## Acceptance Criteria

- Small TCP burst improves by at least 20%.
- `recv()` calls fall by at least 60%.
- 64-KiB parsing does not regress by more than 2%.
- Same-shape isolated-message latency does not regress by more than 5%.
- Partial reads, fairness, TLS pending data, WebSocket ownership, and buffer reset remain correct.

## Before Measurement

Environment: CPython 3.12.3, Linux x86-64, 15 runs where applicable.

| Scenario | Before |
| --- | ---: |
| Local socketpair, 1,000 small PUBLISH | 50,520 msg/s |
| Socket reads for the burst | about 3,000 |
| Same-shape isolated message | 22.6 us |
| 64-KiB brokerless parse | about 7.8k msg/s |

## Implementation

- Added a 64-KiB private read-ahead buffer for built-in TCP, Unix, and TLS loops.
- Added a 100-packet fairness cap per readiness event.
- Short reads mark the transport drained, avoiding a final EAGAIN syscall/exception.
- Buffered bytes count as pending for the next select iteration.
- Public `loop_read()` keeps its existing behavior; WebSocket keeps its own
  frame buffering (the batched parser still requests up to the read-ahead
  chunk from the wrapper, which returns at most one frame per call, so the
  64 KiB chunk sizing only applies to raw stream sockets).
- Socket close/reconnect discards prefetched bytes.

## After Measurements

| Scenario | After | Delta |
| --- | ---: | ---: |
| Local socketpair, 1,000 small PUBLISH | 92,977 msg/s | **+84.0%** |
| Socket reads for the burst | 1 | **-99.97%** |
| Same-shape isolated message | 22.7 us | +0.4% |
| `loop_read_batch_v3_qos0_small` | 74,789 msg/s | regression guardrail |
| 64-KiB brokerless parse | 8,460 msg/s | no regression signal |

## Results Analysis

The main gain comes from amortizing both selector and socket-call overhead. Preserving the short-read state across fairness-limited batches was necessary to avoid an extra EAGAIN probe and its latency cost. The normal packet-at-a-time parser remains at or above its prior MQTT v3 rate.

## Verdict

**GO.** All primary thresholds are met with bounded buffering and explicit fairness tests.
