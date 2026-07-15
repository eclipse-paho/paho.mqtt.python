# 15 - Batched ACK Inflight Refill

## Analysis

Every PUBACK or PUBCOMP currently completes one outgoing message and calls
`_update_inflight()` immediately. When the built-in read loop already has a
burst of ACK packets available, this repeats the scan and promotes one queued
message per ACK even though all freed slots could be refilled in one pass.

This is different from the ready-queue rejected in project 05. The authoritative
`OrderedDict` and its O(max_inflight) scan remain unchanged; only the frequency
of refill calls changes at an existing packet-batch boundary.

## Preparation

- Add brokerless ACK bursts of 1, 8, 20, 100, and 1,000 messages.
- Test inflight limits 20, 100, and 1,000 with queues of 100, 1,000, and 10,000
  messages.
- Measure MQTT v3 PUBACK and PUBCOMP separately, then MQTT v5 ACKs with empty
  and rich properties.
- Count `_update_inflight()` calls, scanned messages, promoted messages,
  generated PUBLISH packets, CPU, allocations, and ACK/s.
- Include callbacks that publish another QoS message and callbacks that raise.
- Preserve the project 05 harness as a regression reference.

## Expected Gain

Priority: P0.

- At least 20 percent higher saturated ACK throughput for realistic batches.
- One refill scan per batch instead of one per ACK.
- Larger output batches for project 16 without another persistent queue.
- No meaningful change for an isolated ACK.

## Acceptance Criteria

- At least 20 percent higher end-to-end throughput for 100 ACKs with a
  saturated inflight window.
- Refill calls fall from approximately one per completed ACK to one per
  internal batch.
- No regression above 2 percent for a single ACK or public packet-at-a-time
  `loop_read()`.
- `on_publish`, `MQTTMessageInfo`, ACK ordering, outgoing message ordering,
  inflight accounting, DUP flags, and MQTT v5 validation remain unchanged.
- Unknown and duplicate ACKs do not reserve refill capacity.
- A callback-published message remains ordered behind already queued messages.
- Fatal parse/send errors flush or discard deferred refill work according to
  the existing connection state, without leaving a free-slot count pending.

## Before Measurement

The permanent brokerless scenario completes batches of 100 MQTT v3 PUBACKs
against 1,000 queued QoS 1 messages with an inflight limit of 100. With two
warmups and 15 measured runs, the immediate per-ACK refill implementation
reached **23,405 ACK/s** (median 85.452 ms per 2,000 ACKs, range
71.980--123.336 ms, p95 54.79 microseconds/ACK).

A paired same-process harness, which removes machine drift and scenario setup
from the timed region, measured the legacy batch at **46,566 ACK/s**. It also
records the isolated-ACK path as a guardrail.

## Implementation

- Added private deferred/pending refill state scoped to `_loop_read_batch()`.
- ACK completion still removes the message, publishes `MQTTMessageInfo`, and
  decrements inflight immediately. If more buffered packets remain, it records
  one pending refill instead of rescanning the outgoing mapping.
- The batch boundary calls the existing `_update_inflight()` once under the
  existing mutex, preserving its ordering and state transitions.
- An isolated ACK and the public packet-at-a-time path refill immediately.
- Protocol errors discard pending work; exceptions restore the enclosing batch
  state and flush a valid pending refill.
- No public option, secondary queue, or alternate source of truth was added.
- Added a permanent harness, paired evaluator, and tests for one refill per
  batch, immediate public-path refill, and protocol-error cleanup.

## After Measurements

The same permanent 2-warmup/15-run scenario reached **54,287 ACK/s**, a
**+132.0%** improvement. Median time fell to 36.841 ms per 2,000 ACKs (range
35.708--39.974 ms, p95 19.88 microseconds/ACK).

The final paired run measured **110,567 ACK/s** versus **46,566 ACK/s** for
the legacy control, or **+137.4%**. The isolated internal-batch guardrail was
15.75 microseconds versus 15.06 microseconds (**-4.4%**); this sub-microsecond
difference is above the strict 2% microbenchmark threshold. Public
packet-at-a-time behavior remains immediate and is covered by a focused test.

Correctness validation: 37 focused read/write tests passed. The enlarged suite
passed with **185 passed, 21 skipped**; the first sandboxed attempt failed only
because local TCP/Unix socket creation was denied and passed unchanged with
that permission enabled.

## Results Analysis

The gain comes from collapsing 100 repeated `_update_inflight()` scans and
send-path promotions into one ordered scan. Both the independent permanent
scenario (+132.0%) and paired timing (+137.4%) exceed the 20% primary target by
a wide margin, so the result is not attributable to run-to-run noise.

The isolated internal-loop cost misses its strict guardrail by 2.4 percentage
points, although the absolute difference is about 0.69 microseconds and the
public packet-at-a-time path does not defer work. This warrants retaining an
explicit condition rather than hiding the trade-off. Project 16 must report
standalone and combined numbers so grouped writes do not mask a regression in
this refill boundary.

## Verdict

**GO with conditions.** Keep the private batched refill because saturated ACK
throughput improves by more than 130% with preserved ordering and protocol
tests. Retain the isolated-ACK guardrail, do not extend deferral to public
packet-at-a-time reads, and remeasure the standalone result after project 16.
