# 19 - Duplex Loop Scheduler

## Analysis

The built-in loop bounds reads to 100 packets but `_packet_write()` drains until
the queue is empty or the socket blocks. A continuously replenished output
queue can therefore delay the next read, ACK handling, callbacks, and
`loop_misc()`. Conversely, a read batch delays generated writes until its end.

A scheduler could bound both directions and alternate them, but every extra
budget check and selector turn is paid by normal QoS traffic. This project must
not exchange a rare starvation improvement for lower publish capacity.

## Preparation

- Add a deterministic duplex transport with 100 inbound packets and 10,000
  already-queued outbound packets.
- Measure first-turn duration, sends per turn, remaining queue, and full drain.
- Test 100-packet, 256-KiB, and 2-ms budgets with alternating first direction.
- Use the independent Mosquitto harness for duplex and QoS publish controls.
- Compare the previous commit and dirty candidate in ABBA order.

## Expected Gain

Priority: P1.

- At least 50 percent lower p95/p99 latency for a starved direction.
- No more than 2 percent loss in pure publish or subscribe throughput.
- No isolated-message or normal QoS exchange penalty.

## Acceptance Criteria

- A duplex direction stops at 100 packets, 256 KiB, or 2 ms.
- Both-ready turns alternate their first direction.
- No timer waits for work; remaining work causes immediate readiness polling.
- At least 50 percent lower p95 or p99 latency in a saturated scenario.
- No unidirectional throughput regression above 2 percent.
- No isolated-message latency regression above 5 percent.
- Public `loop_read()` and `loop_write()` remain unchanged.
- MQTT order remains unchanged and no priority queue is introduced.

## Before Measurement

The brokerless starvation scenario originally drained all 10,000 outgoing
packets in the same call after processing 100 inbound packets. Across 15 runs,
one loop turn took 7--9 ms depending on machine load, performed **10,000
sends**, and left no queued output.

The previously recorded realistic smoke reference reached about 2,013 msg/s in
the burst duplex point and 1,000 msg/s in the rate-limited steady point. Smoke
figures are diagnostic only; production decisions use paired ABBA controls.

## Implementation

Three private prototypes were evaluated and removed:

1. Apply 100-packet/256-KiB/2-ms budgets to every built-in read and write turn.
2. Apply budgets only when both directions are ready.
3. Preserve exact unidirectional behavior and activate duplex scheduling only
   when both directions are ready and output depth exceeds 100 packets.

The prototypes tracked decoded bytes, bounded writer progress, alternated the
first-served direction, and left public manual loop methods unchanged. The
third variant restored the historical read-then-write fast path for normal
queues, but still failed the strict publish guardrail.

All production changes were removed. The retained artifact is
`benchmarks/duplex_scheduler_eval.py`, which reproduces the starvation shape
and will detect contradictory future profiles.

## After Measurements

The fully bounded prototype reduced the synthetic first-turn duration from
about 7--9 ms to **0.85--0.93 ms** and limited output to **100 sends**, leaving
9,900 packets for later turns: an approximately 88--90% fairness improvement.

The same policy increased the synthetic full-drain work through repeated turns
and showed a substantial CPU-only penalty. More importantly, realistic ABBA
QoS 2 publish comparisons found:

| Prototype | Median candidate/baseline | Effect | Verdict |
| --- | ---: | ---: | --- |
| Budgets on every turn | 0.815 | **-18.5%** | regression |
| Both-ready plus queue threshold | 0.878 | **-12.2%** | regression |
| Exact normal fast path | 0.977 | **-2.29%** | inconclusive, over guardrail |

The realistic steady duplex point was unchanged because its offered rate is
capped. A standalone burst smoke run reached 1,625 msg/s, but it was not paired
and therefore cannot establish a latency or throughput win.

After removing the prototype, the focused read/write suite still passes 47/47.

## Results Analysis

The synthetic starvation mechanism is real, and bounding work clearly reduces
the maximum duration of one pathological turn. However, no realistic p95/p99
latency improvement was demonstrated. The independent harness instead found
repeatable publish regressions for the first two variants and an inconclusive
-2.29% signal even after increasingly narrow activation.

The scheduler also adds complexity to keepalive timing, sockpair wakeups,
external registration, partial writes, and error handling. Without a measured
tail-latency failure in a real deployment, that complexity and the remaining
capacity risk are not justified.

This result reinforces the subscribe diagnosis: a new receive-side profile
must first expose client CPU or tail latency rather than a broker/load-generator
ceiling. Scheduler work should not be reopened merely because synthetic queues
can be made pathological.

## Verdict

**NO GO.** Remove all production scheduler changes. Retain only the benchmark,
measurements, and rejection rationale. Reopen the project only with a paired
real workload showing a material p95/p99 starvation problem and a candidate
that keeps every publish/subscribe capacity guardrail within 2 percent.
