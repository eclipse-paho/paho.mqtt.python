# 24 - Automatic MQTT v5 Topic Alias Management

## Analysis

Project 12 rejected an implicit topic-encoding cache because its dictionary
lookup regressed a 1,000-topic publisher by about 14 percent. MQTT v5 Topic
Alias has a different potential benefit: after establishing an alias, repeated
PUBLISH packets can omit the topic from the wire entirely.

Automatic aliasing is still risky because it introduces a lookup on every v5
publish, retains topics, changes wire representation, and must reset correctly
on reconnect. The prototype must therefore be smaller and stricter than a
general cache and must be removed if high-cardinality CPU regresses.

## Preparation

`benchmarks/topic_alias_eval.py` keeps the driver fixed while `--source`
selects the implementation. It feeds a real packed MQTT v5 CONNACK advertising
the requested `TopicAliasMaximum`, then measures public QoS 0 `publish()` calls
with packet queueing isolated from broker and scheduler noise.

The exploratory protocol used two warmups and seven measured runs pinned to CPU
2. Repeated-topic thresholds used short ABBA source order. Scenarios cover one,
8, 16, 17, and 1,000 recurring topics plus all-distinct traffic, broker limits
0 and 16, 16-byte payloads, and topic lengths 8, 32, 128, 256, 512, 1,024,
2,048, 4,096, and 8,192 bytes. The harness records throughput, process CPU,
wire bytes, tracemalloc peak, retained bytes, and admitted aliases.

The prototype also had focused tests for the first definition and later empty
topic, zero broker limit, rich and user-supplied properties, explicit empty
topics, 16-entry/64-KiB saturation, connection reset, and a forced concurrent
publisher interleaving. All ten passed before the rejected production code and
feature-specific tests were removed. A final 15-run campaign was deliberately
not run after multiple mandatory guardrails failed clearly.

## Expected Gain

Priority: P2.

- At least 20 percent fewer wire bytes for long recurring topics.
- At least 5 percent end-to-end throughput gain in a bandwidth- or
  framing-bound repeated-topic scenario.
- No measurable cost when the broker advertises no aliases.
- No high-cardinality churn or retained-memory growth.

## Acceptance Criteria

- Activate only after a successful CONNACK with `TopicAliasMaximum > 0`.
- Bound the table to `min(server limit, 16)` entries and 64 KiB of topic bytes.
- Never evict or replace entries after saturation.
- Reset all aliases on every network connection/reconnection.
- The first use sends full topic plus alias; later uses may send an empty topic
  plus alias.
- User-supplied `TopicAlias` and explicit empty-topic behavior bypass automatic
  management, and user `Properties` are never mutated.
- At least 20 percent fewer wire bytes in the primary repeated-topic scenario.
- Require either at least 5 percent end-to-end improvement or at least 30
  percent fewer wire bytes with CPU regression below 1 percent.
- No throughput regression above 2 percent for 1,000/all-distinct topics.
- No protocol/state regression across QoS 0/1/2, reconnect, replay, properties,
  broker limit changes, or table saturation.

## Before Measurement

Baseline: checkpoint `870d11b`, CPython 3.12, Linux x86-64, 2 warmups and 7
measured runs. Each row queues 10,000 public MQTT v5 QoS 0 publishes with a
16-byte payload and a broker limit of 16. Retained memory is measured after
clearing the packet queue.

Required baseline rows:

| Topics | Topic length | Payload | msg/s | Wire bytes/msg | Retained bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 repeated | 128 | 16 B | 129,643 | 150.0 | 8,672 |
| 8 recurring | 128 | 16 B | 133,851 | 150.0 | 8,672 |
| 16 recurring | 128 | 16 B | 123,205 | 150.0 | 8,672 |
| 17 recurring | 128 | 16 B | 132,561 | 150.0 | 8,672 |
| 1,000 recurring | 128 | 16 B | 129,478 | 150.0 | 8,672 |
| all distinct | 128 | 16 B | 129,828 | 150.0 | 8,672 |

## Implementation

The removed prototype implemented a private per-connection mapping capped at
`min(server maximum, 16)` and 64 KiB, without eviction. The first packet carried
the full topic plus alias; hits carried an empty topic. Packed properties were
augmented without mutating the user's `Properties`, and manually supplied
aliases or explicit empty topics bypassed automation.

A reentrant lock covered alias selection through insertion of the defining
packet into the output queue. This is required: without it, a concurrent hit
can overtake the definition and send an invalid empty topic. Tables reset on
socket close and every CONNACK. Alias-only property encodings were precomputed.

Two attempts reduced regressions without weakening correctness:

- saturated streams stopped synchronized lookups and, after 64 consecutive
  misses, disabled automation for the connection while continuing to send
  valid full topics;
- a 1-KiB minimum topic threshold was evaluated after shorter topics remained
  CPU-negative.

Neither produced a stable broadly useful result. All production and focused
feature-test changes were removed. Only the independent benchmark remains.

## After Measurements

Representative exploratory comparisons follow. Throughput deltas use paired
ABBA medians where available; negative means the prototype is slower.

| Scenario | Baseline wire B/msg | Prototype wire B/msg | Throughput delta | CPU conclusion |
| --- | ---: | ---: | ---: | --- |
| repeated, topic 8 B | 29.0 | 24.0 | -17.5% | clear regression; wire reduction also below 20% |
| repeated, topic 32 B | 53.0 | 24.0 | -17.2% | clear regression |
| repeated, topic 128 B | 150.0 | 24.0 | about -10.8% | about +12% CPU time |
| repeated, topic 256 B | 278.0 | 24.0 | about -5.7% | regression |
| repeated, topic 512 B | 534.0 | 24.1 | about -6.1% | regression |
| repeated, topic 1,024 B | 1,046.0 | 24.1 | +2.2% in one ABBA, -6.1% in the extended ABBA | unstable; never reached +5% |
| recurring 1,000, adaptive cutoff | 150.0 | 150.005 | about -4.3% | mandatory high-cardinality guard failed |
| all distinct, adaptive cutoff | 150.0 | 150.005 | about -0.6% | within guardrail only after extra state |
| broker limit 0 | 150.0 | 150.0 | about +0.3% | neutral |

The favorable 128-byte case removed about 84% of wire bytes, and the 1,024-byte
case removed about 97.7%. For 10,000 queued repeated 1,024-byte topics,
tracemalloc peak fell from 16.3 MiB to 4.65 MiB. The table itself retained about
1.2 KiB for one 1-KiB topic or 17.4 KiB for sixteen topics. Probes at 2, 4, and
8 KiB remained non-monotonic, confirming that no trustworthy size threshold
could be selected from the data.

## Results Analysis

The wire and queued-memory reductions are genuine, but they are not sufficient
to accept an implicit feature. Every alias hit must hash the freshly encoded
topic, consult connection state, inject a property, and synchronize definition
order. Python's existing full-topic copy is native and very efficient, so the
automatic bookkeeping costs more CPU for common topic sizes.

Moving alias selection to the writer could serialize ordering naturally, but
would require a new logical PUBLISH queue representation and delayed packet
construction across QoS replay, segmented payloads, reconnect, TLS, WebSocket,
and external-loop paths. That is disproportionate architecture for a P2 case
whose only promising measurements involve unusually long topics. Sampling,
miss cutoffs, and size thresholds merely add policy and latency variability;
they do not establish a broad win.

This result reinforces project 12 rather than reopening it: an implicit topic
lookup remains visible in public publish CPU. Applications on constrained links
can already use explicit MQTT v5 `TopicAlias` properties, choosing the topics
and lifecycle for which the bandwidth trade-off is worthwhile without imposing
cost or hidden wire changes on every client.

## Verdict

**NO GO.** Do not add automatic MQTT v5 topic aliases. The common 32--512-byte
cases regress construction throughput, the 1-KiB result is unstable and below
the required 5%, and a broadly safe implementation requires concurrency and
adaptive state disproportionate to the niche benefit. Retain only the
brokerless scenario and continue supporting explicit user-managed aliases.
