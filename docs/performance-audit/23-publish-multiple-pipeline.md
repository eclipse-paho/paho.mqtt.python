# 23 - `publish.multiple()` Pipeline

## Analysis

The one-shot `publish.multiple()` helper submitted its first message on
CONNACK and every following message only from `on_publish`. Its effective
window was therefore one. QoS 1 and QoS 2 throughput was limited to roughly
one message per broker round trip even though `Client` already supports an
inflight window of 20 and ordered queueing.

This is an orchestration bottleneck, not a packet-codec bottleneck. A bounded
sliding completion window can preserve the public signature, blocking
behavior, input order, and completion semantics while overlapping broker ACK
latency. It does not reduce MQTT bytes or ACK count; it reduces idle time
between otherwise independent messages.

## Preparation

`benchmarks/publish_multiple_pipeline_eval.py` invokes the public helper with a
deterministic event-model client. It records CPU, elapsed time, virtual wall
time under controlled ACK delays, throughput, `tracemalloc` peak, publish
order, callback count, disconnect timing, and peak outstanding messages. The
timed window excludes cyclic garbage collection to prevent locally-created
fake client classes from adding random GC pauses.

The frozen baseline is `/tmp/paho-plan23-baseline`. Final brokerless results
use two warmups and 15 measured runs pinned to a CPU. Short loopback checks use
the MQTT broker already present on `127.0.0.1:11883`; it passed a real
CONNECT/CONNACK health check, but was owned outside this checkout and was not
isolated from other activity. Its large run-to-run spread is consequently
used only to confirm the order of magnitude, not fine regression thresholds.

`tests/test_publish_multiple_pipeline.py` covers bounded order/completion,
mixed QoS, exact window boundaries, queue rejection, exceptions, synchronous
callback reentrancy, duplicate completions, reconnect, MID wrap, CONNACK
failure, and the different QoS 0 versus QoS 1 behavior of
`MQTT_ERR_NO_CONN`.

## Expected Gain

Priority: P1, narrow helper scope but multiplicative on latency-bound brokers.

- At least fivefold higher QoS 1 throughput with 20-ms ACK delay.
- Up to the existing 20-message inflight factor when ACK latency dominates.
- Lower total CPU in real QoS 1/2 batches by avoiding repeated network-loop
  turns, despite modest extra local state-machine work.
- No reduction in wire bytes or ACK count; at most 20 messages are in progress
  instead of one.

## Acceptance Criteria

- Use one internal fixed submission window of 20 messages, with no public
  parameter, signature, or size-dependent alternate path.
- At least 5x higher throughput for 100 QoS 1 messages with 20-ms ACK delay.
- At least 70 percent lower wall time for the 100-ms-delay scenario.
- No regression above 5 percent for one message or a zero-delay loopback
  helper call; fine loopback claims require an isolated broker.
- At most 20 messages are outstanding simultaneously.
- Preserve input publish order and complete every accepted message exactly
  once before DISCONNECT.
- Handle mixed QoS, queue rejection, callback reentrancy, MID wrap, reconnect,
  and exception cleanup without hangs or premature disconnect.

## Before Measurement

Final deterministic baseline, two warmups and 15 runs:

| Messages | QoS | ACK delay | CPU median | Virtual wall | msg/s | Peak outstanding | Peak bytes |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 0 ms | 0.062 ms | 0 ms | n/a | 1 | 2,280 |
| 100 | 1 | 0 ms | 0.312 ms | 0 ms | n/a | 1 | 13,064 |
| 100 | 1 | 20 ms | 0.298 ms | 2,000 ms | 50.0 | 1 | 13,064 |
| 100 | 1 | 100 ms | 0.332 ms | 10,000 ms | 10.0 | 1 | 13,064 |
| 100 | mixed | 20 ms | 0.291 ms | 1,320 ms | 75.8 | 1 | 13,064 |
| 1,000 | 1 | 20 ms | 2.449 ms | 20,000 ms | 50.0 | 1 | 28,584 |

The brokerless zero-delay result measures only Python bookkeeping and a nearly
empty fake `publish()` call. It is a useful cost probe, not an end-to-end
throughput model.

## Implementation

- Replace the raw userdata deque with a private `_MultipleState` containing
  remaining messages, outstanding count, pending MIDs, reentrancy state,
  terminal error, and one-shot disconnect state.
- Fill up to 20 slots after CONNACK and refill a slot after each valid
  completion callback.
- Track MIDs so duplicate or unknown completions cannot release a slot twice;
  reserve a slot before `publish()` so synchronous custom clients are safe.
- Preserve QoS 1/2 messages returning `MQTT_ERR_NO_CONN`, because `Client`
  keeps those messages queued for reconnect. Treat the corresponding QoS 0
  result as rejected because it is not queued.
- On queue rejection or an exception, stop accepting new work, drain already
  accepted messages, disconnect once, and then raise the original error.
- Keep a single implementation path for every non-empty input. An exploratory
  one-message fast path was removed: it duplicated callback behavior to avoid
  only a few microseconds of bookkeeping and would have required arbitrary
  thresholds and parallel semantics.

## After Measurements

Final deterministic candidate, two warmups and 15 runs:

| Messages | QoS | ACK delay | CPU median | Virtual wall | msg/s | Peak outstanding | Peak bytes |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 0 ms | 0.071 ms | 0 ms | n/a | 1 | 2,848 |
| 100 | 1 | 0 ms | 0.830 ms | 0 ms | n/a | 20 | 18,976 |
| 100 | 1 | 20 ms | 0.827 ms | 100 ms | 1,000.0 | 20 | 18,976 |
| 100 | 1 | 100 ms | 0.510 ms | 500 ms | 200.0 | 20 | 18,976 |
| 100 | mixed | 20 ms | 0.479 ms | 80 ms | 1,250.0 | 20 | 18,976 |
| 1,000 | 1 | 20 ms | 4.569 ms | 1,000 ms | 1,000.0 | 20 | 36,080 |

Controlled results are 20x for QoS 1 at both 20 and 100 ms, with 95 percent
less virtual wall time. The mixed sequence is 16.5x and 93.9 percent shorter.
The private state adds about 5.9 KiB at 100 messages and 7.5 KiB at 1,000;
outstanding tracking remains bounded at 20.

Short loopback samples confirm a substantial application-level effect despite
broker contention: 100-message QoS 1 medians improved by roughly 44--79
percent and QoS 2 by roughly 41--72 percent across usable paired blocks. In
the first stable final pair, process CPU fell from 9.47 to 5.62 ms for QoS 1
and from 16.54 to 11.12 ms for QoS 2. QoS 0 and one-message sub-millisecond
medians moved in both directions as external load changed, so they cannot
support a fine percentage claim on this shared endpoint.

Correctness validation:

- 14 focused helper tests pass.
- 250 autonomous tests pass, with 21 optional integration tests skipped
  because `paho.mqtt.testing` is absent.
- `git diff --check` passes.

## Results Analysis

The primary batch thresholds pass by a wide margin: throughput is 20x versus
the 5x requirement, the 100-ms case is 95 percent shorter versus the 70
percent requirement, order is preserved, and peak outstanding work is exactly
20. This is a latency-overlap gain; network traffic volume is unchanged.

The deterministic fake-client CPU rises by roughly 0.18--0.53 ms per 100
messages and 2.12 ms per 1,000 messages because set membership and refill
accounting are now real work. That probe deliberately omits encoding, sockets,
selector turns, and ACK processing. In loopback QoS 1/2, the avoided
serialized loop turns more than repay the bookkeeping and total process CPU
falls.

The one-message and zero-delay guardrail cannot be resolved to a trustworthy
fine percentage on the non-isolated broker. The final brokerless one-message
pair shows about 9 microseconds of additional bookkeeping; this is a large
relative percentage only because the fake operation itself is almost empty.
The public module already provides `single()` for the one-message use case.
A separate one-message or small-batch path was evaluated and removed because
it would duplicate semantics for negligible absolute benefit. No threshold
search at 2, 5, or 10 messages is justified without a new isolated profile
showing a user-visible regression in the intended multi-message helper.

The main residual risk is therefore validation environment noise, not an
identified protocol or latency defect. Before an upstream release claim, the
one-message and QoS 0 guardrails should be repeated on a broker exclusively
owned by the harness. The production design itself remains bounded and has
explicit reconnect/error tests.

## Verdict

**GO with conditions.** Keep the single bounded-window implementation. It
delivers a large, architecturally expected QoS 1/2 gain and passes the protocol
and memory bounds. Do not add size-dependent helper paths. Before presenting a
strict sub-5-percent one-message/QoS 0 claim upstream, repeat only those short
guardrails on an isolated broker; reopen the implementation only if that test
shows a reproducible user-visible regression rather than sub-millisecond
environment noise.
