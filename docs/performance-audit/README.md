# Paho MQTT Python Performance Audit

This directory contains a performance-oriented audit plan for the Paho MQTT
Python client. It is written as a set of independent project files so each
suspected bottleneck can be profiled, prototyped, accepted, or rejected without
coupling it to unrelated work.

The actual compatibility floor used by this audit and declared by the package
metadata is Python 3.9. Retained production changes must be validated on Python
3.9 and the current Python.

## Project Index

| Project | Priority | Status | Main code paths | Expected outcome |
| --- | --- | --- | --- | --- |
| [01 - Packet Read Parser](01-packet-read-parser.md) | P0 | **Done (this round)** | `Client._packet_read()`, `Client._handle_publish()` | Reduce receive-side copies, allocations, and dictionary lookups. |
| [02 - Packet Write Queue](02-packet-write-queue.md) | P0 | **Done (this round)** | `Client._send_publish()`, `Client._packet_queue()`, `Client._packet_write()` | Improve publish throughput and reduce wakeup/write overhead. |
| [03 - MQTT v5 Properties and Reason Codes](03-mqttv5-properties-reasoncodes.md) | P0 | **Done** | `Properties`, `ReasonCode`, MQTT v5 handlers | Remove repeated metadata construction and linear lookups. |
| [04 - Callback Dispatch and Topic Matching](04-callback-dispatch-topic-matching.md) | P1 | **Partial** (round 1 done; eager `match()` NO GO; `iter_match` micro-opt kept) | `Client._handle_on_message()`, `MQTTMatcher` | Bound callback filtering overhead under many subscriptions. |
| [05 - Inflight Message State](05-inflight-message-state.md) | P1 | **NO GO** (ACK path) | `_out_messages`, `_update_inflight()` | Ready-queue rejected; reconnect O(N) scan noted for future. |
| [06 - Threading Wakeup and Event Loop](06-threading-wakeup-event-loop.md) | P1 | **Done** | socketpair wakeups, locks, loop integration | Wakeup coalesce + tests + state machine doc. |
| [07 - WebSocket Transport](07-websocket-transport.md) | P2 | **Done** | `_WebsocketWrapper` | Hybrid native masking and zero-copy partial-send cursor. |
| [08 - Logging and Observability](08-logging-observability.md) | P1 | **Done (harness)** | `_easy_log()`, benchmark/profiling workflow | Add low-noise measurement and regression guardrails. |
| [09 - Read-Ahead and Packet Batching](09-read-ahead-packet-batching.md) | P0 | **Done** | `_loop()`, `_packet_read()`, `_sock_recv()` | Batch inbound packets and amortize socket reads. |
| [10 - Publish ACK Completion](10-publish-ack-completion.md) | P1 | **Done** | `_handle_pubackcomp()`, `_do_on_publish()` | Skip callback metadata when MQTT v3 has no publish callback. |
| [11 - MQTT v5 Rich Property Codec](11-mqttv5-rich-property-codec.md) | P1 | **Done** | `Properties`, `VariableByteIntegers` | Cursor parsing and native UTF validation. |
| [12 - Outbound Topic Encoding Cache](12-outbound-topic-encoding-cache.md) | P1 | **NO GO** | `Client.publish()` | Rejected due to high-cardinality regression. |
| [13 - Reconnect Reset and Replay](13-reconnect-replay.md) | P2 | **Done** | reconnect reset, CONNACK replay | Remove repeated invariant work without a second queue. |
| [14 - Contiguous Ingress Decoder](14-contiguous-ingress-decoder.md) | P0 | **GO with conditions** | built-in ingress pump, `loop_read()` | Direct buffered decode kept; public batching prototype rejected. |
| [15 - Batched ACK Inflight Refill](15-batched-ack-inflight-refill.md) | P0 | **GO with conditions** | ACK completion, `_update_inflight()` | Refill all slots once per ACK batch. |
| [16 - Transport-Aware Batched Writer](16-transport-aware-batched-writer.md) | P0 | **NO GO** | `_packet_write()`, transport send paths | Real workloads reduce syscalls but do not establish meaningful application gains. |
| [17 - Reconnect Replay Staging](17-reconnect-replay-staging.md) | P1 | **GO with conditions** | successful CONNACK replay | Stage ordered retransmits in bounded drains. |
| [18 - Segmented Outbound Payloads](18-segmented-outbound-payloads.md) | P1/P2 | **GO with conditions** | PUBLISH construction, writer | Avoid copying large immutable payloads. |
| [19 - Duplex Loop Scheduler](19-duplex-loop-scheduler.md) | P1 | **NO GO** | private built-in event loop | Synthetic fairness gain did not pass publish guardrails. |
| [20 - Deadline-Driven Thread Loop](20-deadline-driven-thread-loop.md) | P1 | **GO with conditions** | `loop_start()`, `loop_stop()`, reconnect wait | Adaptive idle deadlines and interruptible lifecycle waits; long CPU/timer validation remains. |
| [21 - WebSocket Inbound Streaming](21-websocket-inbound-streaming.md) | P2 | **GO with conditions** | `_WebsocketWrapper.recv()` / `pending()` | Decode frames from bounded read-ahead buffers. |
| [22 - Callback and State-Lock Decoupling](22-callback-state-lock-decoupling.md) | P1 | **GO with conditions** | PUBACK/PUBCOMP/PUBREL callbacks | Remove callback-induced producer/reset lock latency. |
| [23 - `publish.multiple()` Pipeline](23-publish-multiple-pipeline.md) | P1 | **GO with conditions** | one-shot publish helper | Use a bounded 20-message completion window. |
| [24 - Automatic MQTT v5 Topic Alias](24-mqttv5-automatic-topic-alias.md) | P2 | **NO GO** | CONNACK capabilities, PUBLISH packing | Wire savings do not justify common-case CPU and concurrency cost; use explicit aliases. |
| [25 - TLS Session Resumption](25-tls-session-resumption.md) | P2 | **GO with conditions** | TLS handshake/reconnect | Reuse TLS 1.3 sessions and TLS 1.2 sessions only on preconfigured `TCP_NODELAY` sockets. |
| [26 - Ordered State Dictionaries](26-ordered-state-dicts.md) | P1 | **GO with conditions** | `_out_messages`, `_in_messages` | Cut mapping memory by 53--61% and stable reconnect-scan CPU by about 16%. |
| [27 - Native Socket Pair](27-native-socketpair.md) | P2 | **GO with conditions** | threaded-loop wakeup lifecycle | Use the runtime's portable primitive; Windows Python 3.9 validation remains. |
| [28 - Cold Start and Imports](28-cold-start-imports.md) | P2 | **GO** | module imports, proxy discovery | Avoid 28--30% of cold-start time when PySocks is absent. |

Runtime-floor maintenance and later-runtime ideas are tracked separately in
[Python Runtime Opportunities](python-runtime-opportunities.md).

## Progress Snapshot (2026-07-09)

Landed on branch `benchmarks` (representative commits):

- `238eee8` harness + audit plans (08)
- `f2aaa76` MQTT v5 properties / reason-code metadata cache (03)
- `92008c1` receive dispatch: lazy `MQTTMessageInfo`, filtered-callback fast path (04 round 1)
- `6f6869c` / `65e1671` / `6bb33c5` write path + wakeup coalesce + remaining-length fast path (02, 06 partial)
- plan **01**: reusable `_InPacketState`, index-based `_handle_publish`, v5 empty-props fast path
- plan **04** round 2: eager `match()` rejected; kept `iter_match` micro-opt (`nparts` / `yield from`) + dispatch tests/harness
- plan **05** evaluated: ready-queue **NO GO** on ACK path (scan O(max_inflight)); reconnect reset O(N) deferred
- plan **06** closed: wakeup coalesce (02) + callback/external-loop tests + state machine doc

This PR scope: plans **01–06** and **08** harness. Plan **07** (WebSocket) is explicitly deferred.

Receive follow-up for `mqtt_zigbee_listener` (2026-07-09):

- Harness: `publish_parse_v3_qos2_small`, `publish_parse_v3_qos2_z2m_filters`, `dispatch_z2m_seven_filters`.
- **GO**: skip PUBLISH `print_topic` UTF-8 decode when no log sink (~+9.5% parse, logger off).
- **Kept**: `MQTTMessage._topic_str` cache (match + callback share one decode).
- **NO GO**: further QoS2 `_in_messages` bookkeeping; further matcher/`list()` work for 7 filters (~+2.5%).
- Remaining listener CPU is largely **outside paho** (SQLAlchemy workers / `orjson` / app logging).

Second audit round (2026-07-10):

- **09 GO:** built-in read-ahead/batching improves a local 1,000-message burst by about 84% and reduces burst reads to one.
- **10 GO:** MQTT v3 ACK completion without `on_publish` improves by about 18%.
- **11 GO:** rich property unpack and end-to-end MQTT v5 rich PUBLISH improve by about 53% and 28% in paired runs.
- **12 NO GO:** a bounded topic cache regresses a 1,000-topic publisher by about 14%; prototype removed.
- **13 GO:** QoS 2 reconnect reset improves by about 24% at 1,000 messages.
- **07 GO:** WebSocket frame creation improves about 154% at 128 bytes with bounded 64-KiB masking chunks.

Third architectural audit plan (2026-07-11):

- **P0 core:** projects 14-16 cover contiguous ingress, batch-level inflight
  refill, and transport-aware grouped writes.
- **Flow/lifecycle:** projects 17-20 cover reconnect staging, large immutable
  payload ownership, duplex fairness, and deadline-driven thread wakeups.
- **Transport/concurrency/helpers:** projects 21-25 cover WebSocket ingress,
  callback lock scope, helper pipelining, MQTT v5 aliases, and TLS resumption.
- No new public execution mode or setting is planned. Shared reactors,
  asynchronous callback executors, streaming APIs, and byte-based public
  backpressure remain out of scope.
- Execution order is 14, 15, 16, 17, 18, 19, 20, 22, 23, 21, 24, 25. Each
  project stops after paired measurements for explicit verdict and commit
  approval before the next project starts.

Third audit execution:

- **14 GO with conditions:** direct contiguous buffered decode improves TCP and
  TLS small-message ingress by about 37%; the public batching prototype was
  removed.
- **15 GO with conditions:** one inflight refill per private ACK batch improves
  the permanent 100-PUBACK scenario by about 132%; retain the isolated-ACK
  guardrail after a measured sub-microsecond (~4.4%) internal-loop cost.
- **16 NO GO:** realistic QoS 1 workloads cut network-write calls by 42--80%,
  but throughput stays neutral or statistically unresolved instead of reaching
  the required 15%. The corrected experiment remains on
  `perf/plan16-sendmsg-evaluation` at `2560a02`; no production code is merged.
- **17 GO with conditions:** bounded reconnect replay staging improves the
  1,000-message QoS 1 scenario by about 51% and reduces explicit drains from
  1,000 to 16; the faster but unbounded single-drain variant was rejected.
- **18 GO with conditions:** immutable payload segmentation removes more than
  99.99% of Paho's additional 64-MiB allocation and starts socket writes about
  40 ms earlier. Follow-up ABBA found -7.6% at 16 KiB, so the threshold is now
  1 MiB. Probes at 128/256/512 KiB found no reason to reopen the smaller
  boundaries yet; 512 KiB remains a plausible follow-up after latency and QoS 1
  validation. Smaller, mutable, TLS, and WebSocket paths remain contiguous.
- **19 NO GO:** bounded duplex turns reduce a synthetic 10,000-packet
  starvation interval by about 90%, but realistic ABBA publish controls regress
  from 2.3% to 18.5% depending on activation; all production code was removed.
- **20 GO with conditions:** adaptive idle deadlines cut selector returns by
  93.4%, CPU by 76.2%, and voluntary context switches by 85% for 100 clients in
  the 30-second control. Stop and reconnect-backoff latency become
  sub-millisecond for one client, while final active publish is neutral at
  +0.39%. A ten-minute CPU/RSS and keepalive-drift run remains required.
- **22 GO with conditions:** a 200-ms `on_publish` no longer blocks a
  concurrent QoS 1 producer (p95 200.35 ms to 0.031 ms). Sixteen realistic QoS
  1 ABBA slots are neutral at -0.90% paired throughput with p95 +1.4%, while an
  isolated empty callback exposes the expected lock-handoff cost. One standard
  final validation is deferred until the candidate is explicitly accepted.
- **23 GO with conditions:** `publish.multiple()` now overlaps up to 20
  completions. Controlled 20/100-ms ACK scenarios improve 20x and loopback
  QoS 1/2 batches improve materially, with bounded state and full ordering and
  error tests. Fine one-message/QoS 0 guardrails must be repeated on an
  isolated broker; a size-dependent alternate path was explicitly rejected.
- **21 GO with conditions:** bounded WS/WSS inbound streaming improves the
  one-message-per-frame path by 53% (63% through a real TLS socketpair), cuts
  raw reads by more than 99%, and improves 64-KiB messages by 12%. Wrapper
  buffers peak at 104 KiB; a long external-broker RSS/fairness run remains.
- **24 NO GO:** automatic aliases remove 84--98% of repeated-topic wire bytes
  and reduce queued memory, but regress common 32--512-byte publish construction
  by roughly 6--17%. Results for 1-KiB and larger topics are unstable, while
  concurrency-safe ordering and adaptive high-cardinality avoidance add too
  much policy for a niche already covered by explicit `TopicAlias` properties.
- **25 GO with conditions:** verified TLS 1.3 resumption cuts reconnect wall
  time by 41% on MQTT/TCP and 48% on WSS, with CPU reductions of 38% and 47%.
  TLS 1.2 reuse is strictly conditional on the new raw socket already having
  `TCP_NODELAY`; final runs improve wall time by 69% on TCP and 61% on WSS,
  while the default Nagle path performs no reuse or session extraction.

Python 3.9 runtime audit (2026-07-13):

- **26 GO with conditions:** plain insertion-ordered dictionaries reduce
  shallow state-mapping memory by 53--61% and stable 1,000-message reconnect
  scan CPU by 15.7%. ACK/promotion and replay improve only 1.4--3.0%, so no
  general publish-throughput gain is claimed; standard broker and Python 3.9
  guardrails remain.
- **27 GO with conditions:** runtime-floor cleanup replaces the loopback TCP
  emulation with `socket.socketpair()`. Pair and prepared-client lifecycle fall
  by about 85% and 74% locally, with fewer syscalls and no descriptor leak; no
  MQTT throughput gain is claimed, and Windows Python 3.9 validation remains.
- **28 GO:** lazy `urllib` imports cut no-PySocks client/helper cold-start wall
  and CPU by 28--30%, avoid 25 imported modules, and reduce process peak RSS by
  about 6%. Import plus first real proxy lookup is neutral at +0.74%; explicit,
  environment, `no_proxy`, and default-proxy behavior is covered.
- **Python 3.9 maintenance:** direct typing primitives, built-in generic
  containers, monotonic clock, and modern TLS capabilities replace obsolete
  version fallbacks in a separate non-performance change. Optional-SSL and
  runtime TLS-context fallbacks remain intentionally.
- [Future runtime opportunities](python-runtime-opportunities.md) records
  `eventfd`, free-threading, and deliberately closed architectural ideas so
  they are not reopened without a new profile.

Recommended follow-ups:

1. Optional local-broker TCP/TLS/WS system-CPU profiles.
2. Do not reopen the topic cache without a new profile or explicit opt-in design.
3. Do not reopen rejected 02/05 structures without contradictory evidence.
4. Validate plan 25 on Python 3.9 and a real TLS broker before removing its
   environmental conditions.

Do not reopen rejected 02 tracks (`_OutPacket` slots shim, PUBLISH prealloc, fire-and-forget `MQTTMessageInfo`) unless a new profile contradicts the earlier NO GO evidence.

## Measurement Method

Every project must produce a baseline measurement before any implementation
change is accepted. Measurements should be reproducible on a developer laptop
without mandatory external services.

Use two layers of measurement:

1. Brokerless microbenchmarks for isolated CPU costs.
2. Optional local broker scenarios for end-to-end network behavior.

Minimum metrics:

- Messages per second.
- User and system CPU time.
- Median, p50, and p95 time per message where applicable.
- Allocation count and allocated bytes using `tracemalloc` or an equivalent
  standard-library method.
- RSS when running longer broker scenarios.
- Approximate syscall or wakeup counts when measuring loop behavior.

Minimum run protocol:

- Use 2 warmup runs.
- Use at least 7 measured runs during exploration and 15 for final evidence.
- Report the median, spread, and p50/p95/p99 where relevant.
- Warm up the interpreter before recording.
- Pin scenario inputs: payload size, QoS, protocol version, transport, number of
  subscriptions, inflight limit, and queued message count.
- Compare baseline and prototype in the same environment.

## Execution Checkpoints

Projects 14-25 use a strict sequential workflow:

1. Verify the expected HEAD and inspect all tracked/untracked changes.
2. Add the isolated scenario/tests and record the before measurement.
3. Implement only the current project's private prototype.
4. Run focused correctness tests and paired exploratory/final measurements.
5. Complete all nine document sections and stop for human evaluation.
6. Commit only after an explicit `GO`, `GO with conditions`, or `NO GO`
   decision. Accepted production code is one signed commit; a rejected
   prototype is removed before its signed benchmark/documentation commit.
7. Start the next project only from a committed, reviewed checkpoint.

Unexpected overlapping edits are never overwritten or stashed implicitly. The
untracked `AGENTS.md` and `.cursorignore` files remain untouched.

## Benchmark Harness Shape

The recommended harness is documentation-backed and can later become executable
tests or scripts. It should avoid mandatory third-party dependencies.

Brokerless scenarios:

- Decode MQTT fixed header and remaining length for many small packets.
- Parse inbound PUBLISH packets for MQTT v3 and MQTT v5.
- Pack outbound PUBLISH packets for QoS 0 and QoS 1.
- Pack and unpack MQTT v5 properties with empty, common, and heavy user-property
  sets.
- Match topics against many exact and wildcard filtered callbacks.
- Update inflight state with small, medium, and saturated outgoing queues.

Optional broker scenarios:

- TCP QoS 0 publish-only throughput with small IoT payloads.
- TCP QoS 1 publish with PUBACK latency and inflight pressure.
- TCP subscribe throughput with callback dispatch.
- MQTT v5 publish/subscribe with properties.
- TLS receive and send throughput.
- WebSocket publish/subscribe throughput.

## Go/No-Go Matrix

Use this common decision matrix in each project verdict:

| Verdict | Meaning |
| --- | --- |
| `GO` | Measured gain is meaningful, implementation risk is controlled, and compatibility is preserved. |
| `GO with conditions` | Worth implementing only if the listed risks are addressed or a threshold is met. |
| `NO GO` | Expected gain is too small, measurement is inconclusive, or compatibility risk outweighs benefit. |

Default thresholds:

- P0 projects should target at least 10 percent throughput improvement or at
  least 15 percent CPU reduction in their primary hot path.
- P1 projects should target at least 5 percent end-to-end improvement in a
  relevant workload or a clearly bounded latency/CPU reduction.
- P2 projects should proceed only when the affected transport or feature is
  demonstrably important for users.

## Compatibility Rules

- Preserve public API behavior and callback signatures.
- Preserve supported Python versions.
- Keep optional dependencies optional.
- Do not require a specific broker for brokerless benchmarks.
- Do not optimize by weakening MQTT correctness, TLS behavior, message ordering,
  or QoS guarantees.
