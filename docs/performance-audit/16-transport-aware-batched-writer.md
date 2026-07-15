# 16 - Transport-Aware Batched Writer

## Analysis

`_packet_write()` already drains multiple queued packets, but each packet still
causes a separate `_sock_send()` call. This is expensive for four-byte ACKs and
small telemetry PUBLISH packets, and it prevents the socket, TLS layer, and
WebSocket wrapper from amortizing Python calls and framing work.

A transport-aware writer can submit only data that is already queued, without
waiting for a batch to fill. Plain POSIX sockets can use scatter/gather I/O;
TLS and WebSocket require bounded contiguous coalescence. Completion must still
be accounted per MQTT packet after partial writes.

## Preparation

- Add queue depths 1, 2, 8, 64, 100, and 1,000.
- Measure PUBACK/PUBCOMP and PUBLISH payloads of 16 B, 128 B, 1 KiB, and 64 KiB.
- Exercise TCP, Unix, TLS, WebSocket, and a fallback socket without `sendmsg()`.
- Add fake transports that accept 1 byte, cross exactly one packet boundary,
  cross several boundaries, return EAGAIN, and fail after a partial batch.
- Count Python send calls, kernel sends, TLS writes/records where observable,
  WebSocket frames, CPU, allocations, temporary bytes, throughput, and p95.

## Expected Gain

Priority: P0.

- At least 15 percent higher small-packet TCP throughput.
- At least 20 percent higher small-packet TLS throughput.
- At least 80 percent fewer network-write calls at queue depth 100.
- Lower frame and masking overhead for queued WebSocket packets.

## Acceptance Criteria

- At least 80 percent fewer calls to the underlying write primitive for 100
  queued small packets.
- At least 15 percent higher TCP/Unix throughput and 20 percent higher TLS
  throughput in their primary small-packet scenarios.
- No throughput regression above 3 percent for one isolated packet.
- No regression above 2 percent for 64-KiB PUBLISH packets.
- Temporary coalescing memory stays below 128 KiB per client.
- TCP/Unix `sendmsg()` batches are limited to 64 iovecs or 64 KiB, whichever
  comes first; TLS coalescence is limited to 16 KiB.
- No timer or delay is introduced to wait for more work.
- Partial-write positions, QoS 0 completion, callback order, `MQTTMessageInfo`,
  DISCONNECT close timing, EAGAIN, reconnect, and external write registration
  remain correct.
- Each transport may fall back independently if it fails its threshold.

## Before Measurement

The existing writer submitted every queued packet separately. In paired local
socket measurements, 100 four-byte PUBACK packets required **100 writes** and
reached **421,535 packet/s on Unix** and **190,832 packet/s on TCP**.

The CPU-only brokerless control reached **945,024 packet/s**. This removes
kernel syscall cost and is therefore a useful guardrail against moving work
from the kernel boundary into Python.

## Implementation

The bounded POSIX prototype is isolated on branch
`perf/plan16-sendmsg-prototype`, commit `25d75f9`. It is intentionally absent
from the current `benchmarks` branch.

That branch:

- uses native `sendmsg()` for plain TCP/Unix only;
- limits submissions to 64 iovecs and 64 KiB;
- introduces no timer and never waits for a batch to fill;
- keeps isolated packets, TLS, WebSocket, QoS 0 callback packets, DISCONNECT,
  oversized packets, and unsupported sockets on the legacy path;
- tracks partial writes across packet boundaries and preserves EAGAIN state;
- includes its benchmark harness and focused correctness tests.

Required real-usage validation before considering a merge:

- measure p50/p95/p99 latency at queue depths 1, 2, 8, 64, and 100;
- exercise TCP and Unix with fast, slow, and intermittently blocked readers;
- measure QoS 1 PUBLISH payloads of 16 B, 128 B, 1 KiB, and 64 KiB;
- test partial writes followed by EAGAIN, close, reset, and connection errors;
- verify exact wire order and bytes under concurrent enqueue/drain activity;
- test custom socket adapters exposing missing, incomplete, or failing
  `sendmsg()` implementations;
- reproduce the unchanged-path QoS 0 measurements in paired runs;
- run sustained broker tests and check CPU, RSS, tail latency, reconnect,
  callback ordering, and external-loop write registration.

The prototype was rebased onto the completed audit branch for this validation
on `perf/plan16-sendmsg-evaluation`: commit `86fb6ca` ports the original work,
and `2560a02` restores the exact legacy completion path after realistic tests
exposed its accidental overhead.

## After Measurements

Exploratory results from the isolated branch (two warmups, 15 runs):

| Transport | Before | Prototype | Gain | Writes |
| --- | ---: | ---: | ---: | ---: |
| Unix, 100 PUBACK | 421,535 packet/s | 532,167 packet/s | **+26.2%** | 100 -> 2 |
| TCP, 100 PUBACK | 190,832 packet/s | 511,036 packet/s | **+167.8%** | 100 -> 2 |
| Unix, isolated PUBACK | 6.17 us | 5.78 us | **+6.8%** | 1 -> 1 |

The fake transport without real syscall cost regressed from 945,024 to 659,783
packet/s (**-30.2%**). An unchanged QoS 0 depth-100 scenario also showed a
non-paired **-4.6%** signal, while depth 10,000 showed +3.0%. These conflicting
signals are why the prototype is not enabled on the current branch.

Prototype validation completed so far: 40 focused tests passed; the enlarged
suite passed **188 tests with 21 skipped**.

Real-broker re-evaluation with the fixed client harness:

| Scenario | Throughput effect | Network-write effect | Interpretation |
| --- | ---: | ---: | --- |
| QoS 0 unchanged-path, original port | **-16.31%**, CI excludes zero | not instrumented | real regression caused by an unnecessary completion-helper call |
| QoS 0 unchanged-path, corrected port | **-6.28%**, CI -10.35% to +5.39% | unchanged | inconclusive and still outside the median guardrail; CPU-only control was -0.83% |
| QoS 1 ingress, 10k msg/s | **-0.06%**, CI -1.75% to +0.66% | about **-78%** calls | delivered rate unchanged at fixed load |
| QoS 1 ingress, 15k msg/s | **-0.36%**, CI -1.48% to +0.26% | **-79.7%** calls | no application gain; some runs approach broker saturation |
| QoS 1 publish capacity, 12 A/B runs | **+4.37%**, CI -7.33% to +12.33% | **-42.2%** calls | positive median, not statistically established |
| QoS 1 64-KiB fallback | **+9.35%**, CI -3.79% to +20.72% | unchanged path | wide positive noise on a path the prototype does not optimize |

For QoS 1 publish, median latency moved from 3.56 to 3.52 ms at p50,
6.41 to 5.78 ms at p95, and 8.26 to 6.91 ms at p99. These are encouraging
secondary signals, but their throughput series remains inconclusive and the
accepted-call reduction does not by itself satisfy a user-visible objective.

The realistic instrumentation counts both `send()` and `sendmsg()` without
changing Paho. At 10k QoS 1 ingress, the candidate typically groups about five
PUBACK packets per `sendmsg()`. At publish capacity, individual sends remain
common even though each actual `sendmsg()` groups roughly 35--42 PUBLISH
packets; total write-call reduction is therefore only about 42%.

## Results Analysis

The reduction from 100 writes to 2 is real and explains the strong loopback
gain. No intentional queueing latency exists because only already-queued data
is grouped. Nevertheless, the cost of constructing memoryviews is also real,
and loopback throughput cannot establish that production tail latency,
concurrency, socket-adapter compatibility, and failure behavior are unchanged.

The new evidence explains why the large synthetic result does not transfer.
The synthetic test preloads 100 four-byte packets and guarantees a full batch;
real clients drain promptly and commonly expose only small batches. Reducing
syscalls by 42--80% is real, but the remaining work is dominated by MQTT
handling, callbacks, broker processing, and scheduling, so delivered throughput
is neutral or statistically unresolved.

The initial QoS 0 regression was also real and came from refactoring legacy
completion into a Python helper. The evaluation branch fixes it by restoring
the original inline path, yet the realistic unchanged-path control remains too
noisy to certify the strict guardrail. The 64-KiB fallback's unexplained +9.35%
is further evidence that effects of this size cannot be attributed reliably in
the short broker profile.

There is no justification for adding scatter/gather state, capability gating,
partial multi-packet accounting, and custom-socket compatibility risk when the
primary application metrics do not cross their thresholds. TLS and WebSocket
remain unchanged and are not reopened.

## Verdict

**NO GO.** Do not merge the `sendmsg()` backend. It does not reach the required
15% real TCP/Unix throughput improvement, and its syscall reduction is not a
sufficient product outcome on its own. Preserve the corrected experiment on
`perf/plan16-sendmsg-evaluation` at `2560a02`, plus the realistic scenarios and
socket-call instrumentation, so a future profile with demonstrable
write-syscall domination can revisit the decision without reconstructing the
prototype.
