# 21 - WebSocket Inbound Streaming

## Analysis

Project 07 optimized outbound WebSocket masking and partial sends. The inbound
wrapper still parsed at most one frame per `recv()` call and read its header,
extended length, and payload separately. With the common broker shape of one
MQTT PUBLISH per WebSocket frame, 1,000 messages therefore caused 3,000 raw
reads and 1,000 entries into the MQTT batch loop.

Projects 09 and 14 already request a 64-KiB read from the built-in MQTT loop,
but the old wrapper could not use that request across frame boundaries. A
streaming frame decoder is therefore useful only if it exposes payloads from
multiple complete frames in one call while preserving exact, non-prefetching
behavior for the public incremental `loop_read()` path.

The optimization changes neither WebSocket nor MQTT wire traffic. It reduces
Python/socket crossings, frame-parser restarts, and built-in loop turns.

## Preparation

`benchmarks/websocket_inbound_streaming_eval.py` feeds complete MQTT PUBLISH
packets through `_WebsocketWrapper` and the real `Client._loop_read_batch()`
decoder. It covers 1,000 messages as one packet per frame, 100 packets per
frame, eight continuation frames per packet, seven-byte raw chunks, 100
64-KiB packets, and one packet per frame through a real TLS socketpair.

The frozen baseline is `/tmp/paho-plan21-baseline`. Final results use two
warmups and 15 measured runs pinned to CPU 2. Metrics include throughput,
elapsed/process CPU, raw `recv()` calls, MQTT batch calls, `tracemalloc` peak,
and the combined raw/decoded wrapper high-water mark.

Focused tests cover partial headers, 16/64-bit lengths, exact small reads,
binary/continuation FIN state, interleaved PING, PONG/CLOSE replies, partial
control writes, control/data ordering, invalid masked server frames, invalid
continuations, and existing handshake behavior.

## Expected Gain

Priority: P2, transport-specific but material for continuous WebSocket users.

- At least 30 percent higher small-message WebSocket receive throughput.
- At least 80 percent fewer raw socket or TLS reads.
- Fewer MQTT batch-loop entries under one-message-per-frame and fragmented
  traffic.
- No large-frame regression and bounded buffering.

## Acceptance Criteria

- At least 30 percent higher throughput for 1,000 small MQTT PUBLISH packets
  over WebSocket.
- At least 80 percent fewer underlying reads.
- No regression above 2 percent for a 64-KiB MQTT PUBLISH.
- Combined raw and decoded wrapper buffers remain below 128 KiB outside the
  application-owned input stream.
- Exact small `recv(n)` calls do not prefetch the following WebSocket frame, so
  public external-loop/select behavior is preserved.
- FIN, continuation, server masking, binary opcode, PING/PONG, CLOSE, partial
  headers, extended lengths, EOF, and TLS `pending()` remain correct.
- PONG/CLOSE responses survive partial writes and precede a newly queued MQTT
  data frame.
- No public WebSocket option or signature changes.

## Before Measurement

Final baseline, two warmups and 15 runs:

| Transport/frame shape | MQTT packets | msg/s | CPU median | Raw reads | Batch calls | Peak bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| WS, one MQTT packet/frame | 1,000 | 63,513 | 15.75 ms | 3,000 | 1,000 | 1,751 |
| WS, 100 MQTT packets/frame | 1,000 | 119,004 | 8.42 ms | 40 | 10 | 18,723 |
| WS, eight fragments/packet | 1,000 | 12,417 | 80.53 ms | 24,001 | 7,001 | 2,681 |
| WS, one packet/frame, 7-byte chunks | 1,000 | 20,369 | 49.10 ms | 9,000 | 7,000 | 2,601 |
| WSS, one MQTT packet/frame | 1,000 | 55,142 | 18.13 ms | 3,002 | 1,002 | n/a |
| WS, 64-KiB MQTT packet/frame | 100 | 13,420 | 7.46 ms | 500 | 1 | 337,414 |

The old wrapper's very low small-frame `tracemalloc` peak reflects processing
one frame at a time; it is not evidence of high throughput or low total read
overhead.

## Implementation

- Add independent raw-frame and decoded-MQTT byte buffers with cursors and
  bounded compaction.
- On the built-in 64-KiB request, read and parse all complete frames already
  available, returning immediately at a frame boundary rather than waiting for
  future network data.
- Keep exact small reads exact: header and payload reads do not consume the
  next frame, preserving public external-loop readiness semantics.
- Maintain explicit fragmented-message state and validate RSV, FIN, opcode,
  minimal extended lengths, control-frame size, and the RFC prohibition on
  masked server frames.
- Process interleaved PING/PONG/CLOSE frames without exposing control payloads
  to MQTT.
- Queue control replies behind an in-progress data frame, expose them through
  private `pending_write()`, integrate them with `Client.want_write()` and
  `loop_write()`, and retain unsent suffixes across partial writes.
- Return payloads of frames at least 64 KiB directly from the raw buffer to
  avoid a second large copy through the decoded buffer.
- Keep all public classes, methods, callbacks, and WebSocket options unchanged.

## After Measurements

Final candidate, two warmups and 15 runs:

| Transport/frame shape | MQTT packets | msg/s | Delta | CPU median | Raw reads | Batch calls | Peak bytes | Wrapper buffers |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| WS, one MQTT packet/frame | 1,000 | 97,310 | **+53.2%** | 10.28 ms | 1 | 10 | 134,201 | 90,000 |
| WS, 100 MQTT packets/frame | 1,000 | 118,343 | **-0.6%** | 8.45 ms | 1 | 10 | 133,063 | 88,040 |
| WS, eight fragments/packet | 1,000 | 38,866 | **+213.0%** | 25.73 ms | 1 | 10 | 134,442 | 104,000 |
| WS, one packet/frame, 7-byte chunks | 1,000 | 22,709 | **+11.5%** | 44.04 ms | 7,000 | 1,000 | 1,697 | 48 |
| WSS, one MQTT packet/frame | 1,000 | 89,720 | **+62.7%** | 11.14 ms | 5 | 11 | n/a | bounded by TLS/plain path |
| WS, 64-KiB MQTT packet/frame | 100 | 15,023 | **+11.9%** | 6.66 ms | 300 | 1 | 206,548 | 65,546 |

The primary raw-read count falls by more than 99.9 percent and WSS reads fall
from 3,002 to 5. The deliberately hostile seven-byte source cannot eliminate
the network's chunk lower bound but still reduces reads by 22.2 percent and
improves throughput by 11.5 percent.

The candidate holds more data concurrently for small bursts: the total
`tracemalloc` peak rises to roughly 134 KiB. The actual wrapper raw plus
decoded buffers remain bounded at 90 KiB in the primary case and 104 KiB in
the worst fragmented case, below the 128-KiB criterion. For 64-KiB messages,
direct payload return lowers total peak memory by about 39 percent.

Correctness validation:

- 78 focused read/write/WebSocket tests pass.
- 96 focused tests including real local WebSocket handshake fixtures pass.
- The autonomous run has 261 passes and 21 optional skips. The previously
  known timing-sensitive Unix callback test missed `on_disconnect` in the full
  run and passed immediately when rerun in isolation; it does not exercise the
  WebSocket transport.
- `git diff --check` passes.

## Results Analysis

The main throughput threshold passes comfortably: +53.2 percent
versus the required +30 percent. Raw reads fall far beyond the 80-percent
target on both WS and WSS. CPU falls by 35 percent in the primary case, 39
percent under WSS, and 68 percent under fragmentation.

The result depends on frame shape as predicted. A broker already placing 100
MQTT packets in one frame had little overhead to amortize and is neutral at
-0.6 percent. One-message-per-frame and fragmented traffic benefit strongly.
No receive-side delay is introduced: after one raw read, the wrapper returns
at the next available frame boundary and never waits to fill its 64-KiB target.

The first prototype copied every large payload through the decoded buffer and
regressed 64-KiB throughput by roughly 5 percent. Direct large-frame return
removed that copy; the final result is +11.9 percent, with lower memory. This
also demonstrates why a single generic buffering path was insufficient.

Public incremental behavior is protected by the exact-read mode and a test
showing that `recv(1)` leaves the next frame on the underlying socket. Internal
decoded/raw pending bytes wake the built-in loop, while TLS's own `pending()`
continues to participate. Control replies use the existing writable-loop
lifecycle and remain ordered relative to MQTT data frames.

The remaining validation gap is a long-running external WebSocket broker/RSS
profile; the repository's managed Mosquitto configuration exposes raw MQTT and
TLS but no WebSocket listener. The deterministic full MQTT decode path, actual
TLS socketpair, handshake integration tests, strict buffer high-water marks,
and large-frame guardrail provide sufficient evidence for acceptance without
claiming that missing long-duration profile.

## Verdict

**GO with conditions.** Keep the bounded streaming decoder: it materially
improves the intended WS/WSS workloads, fixes fragmented/control-frame state,
and passes all throughput, large-payload, memory, and correctness thresholds.
Before an upstream release claim, run one long RSS/fairness scenario against
an exclusively owned broker with a WebSocket listener. Remove or revise the
implementation only if that real transport run reveals buffer retention or
latency not present in the bounded harness.
