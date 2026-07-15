# 26 - Ordered State Dictionaries

## Analysis

The client stores outgoing and incoming QoS state in two private
`collections.OrderedDict` instances. Every observed operation uses ordinary
mapping behavior plus insertion-order iteration: lookup, assignment, `pop`,
`clear`, membership, and iteration over `values()`. No call relies on
`move_to_end()`, `popitem(last=...)`, equality ordering, or another
`OrderedDict`-specific contract.

Insertion order is a language guarantee for `dict` on the actual Python 3.9
floor. Replacing only these two containers can therefore remove linked-list
bookkeeping and reduce retained memory without reopening the rejected ready
queue from plan 05 or changing the MQTT state machine.

## Preparation

- Inventory every `_out_messages` and `_in_messages` operation and add an
  explicit regression test if any ordering dependency is not already covered.
- Extend the inflight/reconnect benchmark to select `dict` or `OrderedDict`
  without changing the measured ACK and replay code.
- Measure 20, 100, 1,000, and 10,000 queued messages, including QoS 1 ACK,
  QoS 2 transitions, reconnect reset, and successful-CONNACK replay.
- Record container size, Python allocations, CPU time, throughput, and state
  order. Use two warmups and seven runs while tuning, then fifteen paired runs
  for final evidence, pinned to one CPU where sub-millisecond noise matters.
- Run the realistic fixed client harness as a guardrail rather than inferring
  end-to-end throughput from the isolated mapping benchmark.

## Expected Gain

Priority: P1.

An exploratory 100,000-operation insert/pop probe took 60.4 ms with
`OrderedDict` and 37.8 ms with `dict`, about 37 percent less time. At 10,000
entries, the shallow container size fell from 746 KiB to 295 KiB, about 60
percent. A dynamic prototype improved the isolated 1,000-message ACK path by
about 12.7 percent and reconnect reset by about 19.8 percent. These are
directional results only; the 10,000-message replay result was inconclusive.

The expected production benefit is lower CPU and memory for clients retaining
many QoS messages. Network traffic and protocol round trips are unchanged.

## Acceptance Criteria

- Improve ACK completion or reconnect reset/replay at 1,000 messages by at
  least 8 percent in the final paired measurement.
- Reduce shallow mapping memory at 1,000 and 10,000 entries by at least 20
  percent.
- Do not regress the 20-message case or relevant realistic workload by more
  than 2 percent.
- Preserve insertion, deletion, reinsertion, inflight promotion, reconnect
  replay order, QoS 1/QoS 2 states, DUP bits, callbacks, and locking behavior.
- Change only the two private state mappings; do not add a second queue or
  modify any public API.

## Before Measurement

The definitive baseline is commit `620e615`, the committed Python 3.9 metadata
checkpoint. Baseline and candidate used CPython 3.12.3 on CPU 2. Generic
brokerless paths used three ABBA blocks with five raw samples per invocation,
giving 30 samples per version. The saturated ACK/promotion path used eight ABBA
invocations containing three measured loops each. The final stable reconnect
scan used 30 samples per version, 200 scans per sample, after 200 warmup scans.

Exploratory evidence that motivated the plan, but is not used for the verdict:

| Probe | `OrderedDict` | `dict` | Indicative delta |
| --- | ---: | ---: | ---: |
| 100,000 insert/pop operations | 60.4 ms | 37.8 ms | -37.5% |
| Shallow size, 10,000 entries | 746 KiB | 295 KiB | -60.5% |
| ACK throughput, 1,000 messages | 35,530/s | 40,058/s | +12.7% |
| Reconnect reset, 1,000 messages | 7.94/s | 9.52/s | +19.8% |

## Implementation

- Replaced the initial `_out_messages` and `_in_messages` `OrderedDict`
  instances with plain insertion-ordered dictionaries.
- Recreate `_in_messages` as a plain dictionary when a clean reconnect drops
  incoming state.
- Added a regression test for concrete container type, non-sorted insertion
  order, delete/reinsert order, and clean-session reset.
- Extended `inflight_saturation_eval.py` with a stable reconnect scan that
  excludes client/message population and with shallow mapping-size reporting.
- Corrected that legacy benchmark's reset-rate unit: one timed operation
  contains several resets, so runs per second must be multiplied by the reset
  count, not divided by it. Raw ABBA timings used for this plan were unaffected.
- Did not alter locks, state transitions, iteration sites, inflight promotion,
  replay staging, callbacks, public APIs, or the rejected ready-queue design.

## After Measurements

The broad brokerless ABBA matrix is deliberately reported in full, including
the weak and initially contradictory rows:

| Scenario | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| ACK, 20 messages | 65,934/s | 72,106/s | +9.36% |
| ACK, 3,000 messages, no promotion | 97,782/s | 102,533/s | +4.86% |
| ACK plus inflight promotion, N=1,000 | 41,701/s | 42,298/s | +1.43% |
| Reconnect replay QoS 1, N=1,000 | 150.10/s | 154.63/s | +3.02% |
| Generic reconnect reset QoS 2, N=1,000 | 3,015/s | 2,876/s | -4.61% |

The 20-message samples lasted less than one millisecond and had strongly
overlapping ranges, so their positive median is only a guardrail, not a gain
claim. The generic reset row also had strongly overlapping distributions and
included client/message construction in a short timed phase. It triggered a
more controlled measurement instead of being dismissed.

The stable reset probe excludes setup and repeats the actual 1,000-message scan
200 times per sample:

| Stable reconnect reset | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| Median scan throughput | 2,995.76/s | 3,552.98/s | **+18.60%** |
| Median CPU per reset | 333.742 us | 281.461 us | **-15.66%** |

Shallow container memory, excluding identical `MQTTMessage` objects:

| Entries | `OrderedDict` | `dict` | Delta |
| ---: | ---: | ---: | ---: |
| 20 | 1,592 B | 632 B | -60.3% |
| 100 | 10,000 B | 4,688 B | -53.1% |
| 1,000 | 85,400 B | 36,952 B | -56.7% |
| 10,000 | 746,128 B | 294,992 B | -60.5% |

A short external-broker smoke guardrail produced two observations per version
for each QoS 1 inflight window. Candidate medians were -5.4% at inflight 1,
+28.2% at inflight 20, and +13.6% at inflight 100. Baseline observations were
themselves widely dispersed, especially at inflight 100. The harness marks the
smoke profile non-comparable; these values establish neither a regression nor
an end-to-end gain and are not used for acceptance.

Correctness validation under Python 3.12.3:

```text
66 targeted tests passed
276 main tests passed, 21 skipped
```

The longer `tests/lib` subprocess integration group exceeded the execution
window; the main suite includes all unit, fake-broker, WebSocket, state, and
performance regression tests relevant to this private container change.

## Results Analysis

The initial CPU expectation was overstated. Plain dictionaries do not improve
all ACK and replay work by 8 percent: final ACK/promotion and replay gains are
only 1.4 and 3.0 percent. Those paths spend most of their time parsing packets,
updating MQTT state, constructing writes, and scanning the bounded inflight
window rather than maintaining the mapping.

The reconnect result is workload-sensitive but material when measured
correctly. Repeated full-map iteration is 18.6 percent faster and consumes 15.7
percent less CPU. The earlier -4.6 percent row is retained above because it
exposed a benchmark problem; its setup-contaminated, overlapping samples do
not contradict the longer stable scan.

The most general benefit is memory: the two private mappings retain 53--61
percent fewer shallow bytes. This matters for clients with large offline or
QoS queues and comes without a cache, duplicate structure, policy threshold,
or public behavior change. Network bytes and round trips are exactly
unchanged.

Python 3.9 guarantees dictionary insertion order. Existing replay and QoS 2
tests plus the new delete/reinsert test preserve the only ordering semantics
used by the client. No `OrderedDict`-specific operation existed in production.

The realistic smoke result is honestly inconclusive. A standard isolated
broker comparison is still desirable as a release guardrail, but it is not a
reason to discard the demonstrated memory and reconnect-scan improvements.

## Verdict

**GO with conditions.** Retain the plain dictionaries. The stable reconnect
path exceeds the 8 percent CPU threshold, mapping memory exceeds the 20 percent
threshold by a wide margin, the small-queue median does not regress, and all
relevant correctness tests pass.

Do not claim a general ACK or publish-throughput improvement: those final gains
are small, and the realistic smoke run is non-comparable. Before upgrading the
verdict to unconditional `GO`, run the standard QoS 1 inflight comparison on
an isolated broker and execute the focused suite on the actual Python 3.9
floor. Revert the change if either shows a repeatable regression above 2
percent or any ordering/state difference.
