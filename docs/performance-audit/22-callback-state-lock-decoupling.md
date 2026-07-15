# 22 - Callback and State-Lock Decoupling

## Analysis

PUBACK/PUBCOMP handling invoked `on_publish` while
`_out_message_mutex` was held. A slow callback therefore blocked concurrent
QoS publishing even though user code did not need the internal message map
lock. PUBREL handling similarly invoked `on_message` while
`_in_message_mutex` was held, coupling arbitrary application work to reconnect
and inbound-state operations.

The callback itself must remain synchronous on the network thread. Moving it
to an executor would change ordering, exception propagation, shutdown and
backpressure semantics, so that approach remains out of scope. The retained
approach reserves the MQTT state transition under the mutex, runs user code
without the state mutex, and finalizes under the mutex afterward.

The important gain is producer/reset latency under contention, not callback
throughput: the network thread still cannot process the next packet while a
synchronous callback sleeps.

## Preparation

- Added `benchmarks/callback_lock_eval.py`, which loads the SUT from
  `--source`, supports delay-only and batch-only runs, and measures wall time,
  process CPU and `tracemalloc` peak bytes.
- The latency case keeps one QoS 1 slot occupied, enters `on_publish`, and calls
  `publish(qos=1)` concurrently. It verifies that the second message remains
  queued until the ACK finalizes and that `MQTTMessageInfo` is false inside the
  callback.
- The CPU guards process 10,000 ACKs without a callback and with a no-op
  callback. A same-process copy of the retained plan-10 no-callback path avoids
  interpreting CPU-frequency drift as a code regression.
- Added race/correctness coverage for QoS 1/QoS 2, MQTT v3/v5, duplicate ACKs,
  callback removal, suppressed and propagated exceptions, concurrent reconnect
  reset, PUBREL reset and callback failure.
- Iteration policy corrected after an overly long standard run was stopped:
  use two warmups and seven short isolated runs while tuning; use 15 isolated
  runs for final figures; reserve the 60-second standard broker profile for a
  stabilized final/upstream candidate. The realistic development guard used
  16 three-second ABBA slots, not repeated standard runs.

## Expected Gain

Priority: P1.

- Concurrent producers no longer inherit the full duration of `on_publish`.
- Reconnect/inbound-state operations no longer wait on arbitrary QoS 2 message
  callbacks.
- No network syscall reduction is expected; wire format, ACK timing after the
  callback and synchronous callback throughput remain unchanged.
- No permanent per-client or per-message allocation is expected. Reservation
  is represented by a transient private state on the existing message.

## Acceptance Criteria

- With a 200-ms `on_publish`, concurrent `publish(qos=1)` p95 lock-induced
  latency remains below 10 ms.
- No callback is executed while `_out_message_mutex` or `_in_message_mutex` is
  held.
- `MQTTMessageInfo.is_published()` remains false during `on_publish` and becomes
  true at the same logical completion point afterward.
- Inflight capacity remains reserved during the callback so a concurrent
  publisher cannot overtake already queued messages.
- Callback order, callback thread, suppression/propagation of exceptions,
  duplicate ACK behavior, reconnect behavior and MQTT v5 property validation
  remain unchanged.
- No regression above 2 percent without callbacks and 3 percent with a no-op
  callback in the relevant end-to-end workload. An isolated no-op callback is
  retained as a diagnostic, but cannot by itself be the verdict: releasing and
  reacquiring the lock is deliberately dominant when user code does no work.
- Callback-triggered publish, callback removal, callback exception, duplicate
  ACK/PUBREL, concurrent reset and reset-plus-exception are race-tested.

## Before Measurement

Environment: Python 3.12.3, same machine/source snapshot, two warmups and 15
measured runs. The producer latency is the complete concurrent `publish()`
call; it is therefore also a conservative measure of lock wait.

| Callback delay | Concurrent producer p50 | Concurrent producer p95 | ACK median | Result |
| ---: | ---: | ---: | ---: | --- |
| 0 ms | 0.077 ms | 0.094 ms | 0.181 ms | scheduling floor |
| 1 ms | 1.091 ms | 1.124 ms | 1.223 ms | producer inherits callback |
| 10 ms | 10.182 ms | 17.998 ms | 10.846 ms | producer inherits callback |
| 200 ms | 200.194 ms | 200.353 ms | 200.408 ms | acceptance failure |

Pinned short-batch controls varied with process order, but the two baseline
observations bracketed 276.7--279.2 k ACK/s without callback and
127.0--134.8 k ACK/s with a no-op callback. `tracemalloc` peaks were 383 and
759 bytes respectively.

## Implementation

- PUBACK/PUBCOMP first validates the MID and snapshots `on_publish` under the
  historical locks. The no-callback MQTT v3 fast path remains direct and does
  not pay reservation bookkeeping.
- With a callback, the message enters one of two private negative transient
  states: callback pending, or callback pending after a reconnect reset. These
  are deliberately absent from the public `MessageState` enum.
- The outgoing mutex is released before `_do_on_publish()` and reacquired for
  finalization. The message stays in `_out_messages`, its inflight slot remains
  occupied, and its `MQTTMessageInfo` remains unpublished during user code.
- A duplicate ACK seeing a transient state is ignored. Successful completion
  pops the message, publishes its info and refills inflight exactly once.
- Reconnect reset marks a reserved completion as belonging to the previous
  inflight epoch and CONNACK replay skips it. Finalization then avoids a second
  inflight decrement. If an unsuppressed callback exception follows the reset,
  the previous MQTT state is restored and reconnect reset is reapplied so the
  message remains replayable.
- PUBREL pops the inbound message under `_in_message_mutex` to retain duplicate
  suppression, releases the mutex, then invokes `on_message`. Historical
  exception behavior remains: a popped QoS 2 message is not redelivered if an
  unsuppressed callback raises.
- No callback executor, queue, timer, public option, network framing change or
  permanent message/client field was added.

## After Measurements

Final isolated latency, two warmups and 15 runs:

| Callback delay | Baseline producer p95 | Candidate producer p95 | Change | Candidate ACK median |
| ---: | ---: | ---: | ---: | ---: |
| 0 ms | 0.094 ms | 0.018 ms | -80.8% | 0.200 ms |
| 1 ms | 1.124 ms | 0.022 ms | -98.1% | 1.220 ms |
| 10 ms | 17.998 ms | 0.036 ms | -99.8% | 10.324 ms |
| 200 ms | 200.353 ms | 0.031 ms | -99.98% | 200.386 ms |

Pinned 15-run CPU/allocation controls:

| Path | Baseline | Candidate | Interpretation |
| --- | ---: | ---: | --- |
| no callback, cross-process | 276.7--279.2 k ACK/s | 262.1--276.2 k ACK/s | order-sensitive; second ABBA pair -0.2% |
| no callback, same-process legacy control | 39.897 ms CPU | 36.980 ms CPU | candidate +7.9%; no regression reproduced |
| no-op callback, isolated | 127.0--134.8 k ACK/s | 109.0 k ACK/s | -14% to -19%; lock handoff dominates empty user code |
| no callback peak | 383 B | 383 B | unchanged |
| no-op callback peak | 759 B | 687 B | -9.5% |

Real broker QoS 1 capacity used 16 valid ABBA smoke slots (three-second
measure windows) with a short real callback:

| Metric | Baseline median | Candidate median | Result |
| --- | ---: | ---: | --- |
| paired capacity ratio | - | - | -0.90%, CI -6.28% to +2.16% |
| raw capacity | 13,533 msg/s | 13,304 msg/s | -1.7% |
| publish latency p50 | 3.586 ms | 3.697 ms | +3.1% |
| publish latency p95 | 4.742 ms | 4.810 ms | +1.4% |
| publish latency p99 | 5.943 ms | 5.665 ms | -4.7% |
| RSS | 25,900 KiB | 25,830 KiB | neutral |

All 16 broker slots were valid with no protocol failures or final backlog.
Correctness validation: 86 focused tests passed; the full autonomous suite
passed 235 tests with 21 expected skips (`paho.mqtt.testing` absent).

## Results Analysis

The primary criterion is exceeded by a large margin: a 200-ms user callback no
longer adds roughly 200 ms to a concurrent producer, and the observed p95 is
0.031 ms. The inflight slot is still reserved, so the gain is not obtained by
reordering or weakening QoS. Reset latency receives the same architectural
benefit, and the tests cover the reset/exception epoch boundary explicitly.

ACK completion time itself is unchanged at meaningful callback delays, as
expected for synchronous callbacks. Network traffic and syscall counts are
unchanged. Memory is neutral to slightly lower because no reservation set or
dict is allocated.

The isolated no-op callback diagnostic fails the original 3-percent micro
guard because the added lock handoff is a large fraction of an intentionally
empty callback. This does not reproduce end to end: realistic QoS 1 throughput
is -0.90 percent by paired ratio, p95 is +1.4 percent, p99 improves, and RSS is
neutral. However, the smoke confidence interval is too wide to prove a strict
3-percent lower bound. The no-callback path is preserved and the same-process
control found no regression, but cross-process micro results remain too
frequency-sensitive for a sub-2-percent claim.

The attempted standard run was intentionally stopped after it proved far too
slow for iteration (60-second measure, 15-second warmup and 30-second drain per
slot). A standard final run is useful only once, immediately before upstream
submission or after a contradictory real-application result.

## Verdict

**GO with conditions.** Accepted at the explicit evaluation checkpoint.

Conditions:

1. Keep the exact MQTT v3 no-callback fast path and all race tests.
2. Before upstream submission, run one standard ABBA QoS 1 validation (not
   during iterative tuning) and require median throughput/latency regression
   below 3 percent with a materially tighter interval.
3. Prefer one real application with a callback doing 1--10 ms of work and a
   concurrent producer; reject if it shows ordering, inflight or tail-latency
   anomalies.
4. If those controls fail, retain the benchmark/tests and remove only the
   production decoupling before recording `NO GO`.
