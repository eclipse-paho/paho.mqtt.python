# 17 - Reconnect Replay Staging

## Analysis

Project 13 optimized reconnect state reset and invariant work, but successful
CONNACK replay still queues and drains eligible messages inside the iteration.
Calling `loop_write()` after each retransmit prevents the output queue from
forming a useful batch and repeats writer/registration bookkeeping.

Replay can preserve the same ordered state transitions while staging all
currently eligible retransmits first, then invoking one drain. This project is
about replay execution, not the already-accepted O(N) reset pass.

## Preparation

- Preserve project 13 reset measurements as regression guards.
- Add actual CONNACK-to-wire replay scenarios for 100, 1,000, and 10,000
  messages.
- Measure QoS 1 wait-for-PUBACK, QoS 2 publish/PUBREL states, mixed queues, and
  inflight limits 20, 100, and unlimited.
- Run once against the legacy writer and once after project 16.
- Count `loop_write()` calls, underlying writes, state transitions, duplicate
  flags, CPU, allocations, replay completion time, and peak queue memory.

## Expected Gain

Priority: P1.

- At least 20 percent faster replay of 1,000 messages.
- One explicit drain per replay pass rather than one per eligible message.
- At least 80 percent fewer underlying writes when combined with project 16.
- No change to reset complexity or persistent-session semantics.

## Acceptance Criteria

- At least 20 percent lower CONNACK-to-replay-queued time at 1,000 messages.
- One explicit replay drain for each successful CONNACK pass.
- With project 16, at least 80 percent fewer underlying writes for the same
  replay.
- State, order, inflight count, DUP bit, PUBREL handling, callbacks, and
  `MQTTMessageInfo` behavior remain unchanged.
- A send failure stops staging/drain with the same return code and leaves the
  remaining authoritative messages reconnectable.
- No regression above 2 percent in the project 13 reset-only scenario.

## Before Measurement

The new brokerless CONNACK-to-wire scenario replays 1,000 QoS 1 messages with
an inflight limit of 1,000 and a fake socket that accepts complete writes. The
legacy implementation called `loop_write()` **1,000 times**.

With two warmups and 15 runs it reached **104.47 replay/s**: median 191.449 ms
for 20 reconnect passes, range 183.169--244.514 ms, p95 11.72 ms/pass.

The project 13 reset-only guardrail remains structurally untouched. Its current
QoS 2/1,000-message result is 4,412 reset/s (median 22.666 ms per 100 passes).

## Implementation

The first prototype staged all eligible packets and drained once. Although it
improved the primary scenario by about 77%, it was rejected because unlimited
inflight plus large payloads could materialize an unbounded output queue.

The retained prototype instead:

- preserves the authoritative `OrderedDict`, iteration order, state changes,
  timestamps, inflight accounting, and existing per-packet constructors;
- relies on the existing `_in_callback_mutex` guard to queue each replay packet
  without an immediate recursive `loop_write()`;
- drains after at most **64 packets or an estimated 64 KiB**, whichever comes
  first, then performs a final drain;
- introduces no timer and never waits for additional packets;
- preserves historical best-effort semantics by ignoring replay
  `loop_write()` return codes;
- flushes already-staged packets before returning a packet-construction error;
- stops unchanged at the first `mqtt_ms_queued` message.

Tests cover packet and byte ceilings, exact wire order, DUP, QoS 1 states, QoS
2 PUBLISH/PUBREL states, inflight accounting, and construction failure.

## After Measurements

The final bounded implementation reaches **157.53 replay/s**, a **+50.8%**
gain. Median time falls to 126.962 ms per 20 reconnect passes (range
120.855--142.067 ms, p95 7.01 ms/pass).

For 1,000 small messages, explicit replay drains fall from **1,000 to 16**
(-98.4%). The fake transport still performs one underlying send per packet;
therefore the measured gain belongs to replay staging and is not attributed to
the isolated project 16 `sendmsg()` prototype.

Focused replay/property tests passed, the independent client-benchmark unit
suite passed 17/17, and the enlarged library suite passed **190 tests with 21
skipped**.

## Results Analysis

The 50.8% improvement exceeds the 20% target while retaining bounded memory.
It comes from avoiding repeated public `loop_write()` bookkeeping and mutex/time
updates, not from fewer kernel writes. Combining it with project 16 may later
reduce sends, but that prototype is deliberately isolated pending realistic
validation and is not required for this verdict.

The original criterion of exactly one drain per successful CONNACK is rejected
as unsafe for unlimited inflight and large payloads. A maximum of 64 packets or
approximately 64 KiB gives most of the gain without adding unbounded temporary
ownership. Large individual packets are drained immediately after staging and
do not wait for another message.

The realistic smoke report confirms large publish-side gains elsewhere, but it
does not contain a reconnect storm workload; this project still needs an
optional broker-backed reconnect/replay validation before upstream submission.

## Verdict

**GO with conditions.** Keep bounded replay staging, not the unbounded
single-drain prototype. Before upstream submission, validate a persistent
session reconnect against a real broker with mixed QoS 1/QoS 2, slow readers,
large payloads, and forced disconnects. Any state, DUP, ordering, callback, or
tail-latency difference changes the verdict to `NO GO`.
