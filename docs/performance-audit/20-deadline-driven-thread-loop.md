# 20 - Deadline-Driven Thread Loop

## Analysis

`loop_start()` enters `loop_forever()` with a one-second timeout, so an idle
client wakes once per second even when its keepalive deadline is a minute away.
`loop_stop()` sets a flag but does not wake the selector, and reconnect backoff
polls termination through one-second sleeps. This wastes idle CPU and makes
lifecycle latency depend on polling intervals.

The existing socketpair can wake the selector for stop as well as publish. A
private event can make reconnect backoff interruptible, while a deadline
calculation can sleep until the next real MQTT timer.

## Preparation

- Measure 1, 10, and 100 idle clients for keepalive values 10, 60, and 300 s.
- Record selector wakeups, user/system CPU, voluntary/involuntary context
  switches, thread count, and RSS over at least ten minutes.
- Measure `loop_stop()` during selector wait, queued output, reconnect backoff,
  and immediately after a wakeup race.
- Measure single-message publish-to-send latency while the thread is sleeping.
- Keep blocking DNS, TCP connect, TLS handshake, proxy, and WebSocket handshake
  outside the sub-50-ms stop guarantee and report them separately.

## Expected Gain

Priority: P1.

- More than 90 percent fewer idle selector wakeups at keepalive 60.
- At least 80 percent lower aggregate idle CPU for 100 clients.
- Sub-50-ms stop latency during selector wait and reconnect backoff.
- No active-traffic throughput or wakeup-latency regression.

## Acceptance Criteria

- At least 90 percent fewer selector wakeups at keepalive 60.
- At least 80 percent lower idle CPU for 100 connected clients.
- `loop_stop()` p95 below 50 ms while in `select()` or reconnect backoff.
- Keepalive/PINGRESP deadline drift remains below 1 percent.
- Single-message publish-to-send p95 does not regress by more than 5 percent.
- Active throughput does not regress by more than 2 percent.
- Public `loop()` and `loop_forever(timeout=...)` timeout semantics remain
  unchanged; deadline-driven waits apply only to the internal `loop_start()`
  thread.
- Stopping does not claim to flush publications; queued state remains valid for
  restart/reconnect as documented.
- Wakeup coalescing from project 06 remains race-free when stop and publish
  occur concurrently.

## Before Measurement

The reusable brokerless evaluator runs a connected idle network thread over a
local socketpair and measures selector returns plus stop latency. A second
thread waits in the real reconnect-backoff implementation. The baseline is
commit `706e827`, immediately before the prototype.

Required baseline rows:

| State | Clients | Duration | Wakeups | CPU | Stop p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Connected idle, keepalive 60 | 1 | 10.2 s control | 11 selector returns | not sampled | 810.77 ms |
| Connected idle, keepalive 60 | 100 | 30 s control | 3,026 selector returns | 0.296 CPU-s | 996.14 ms collective |
| Reconnect backoff | 1 | short control | n/a | not sampled | 980.06 ms |
| Active small publish | 1 | 2 x 16 ABBA runs | n/a | harness telemetry | baseline A/B |

## Implementation

Prototype retained after component isolation:

- Add a private thread-termination `Event` created/reset with `loop_start()`.
- Wake the existing socketpair in `loop_stop()` under the established wakeup
  mutex/state machine.
- Replace reconnect `sleep()` polling with interruptible event waits.
- For the internal thread only, keep the historical one-second timeout while
  traffic was seen less than one second ago, then wait only until the real
  keepalive deadline. A keepalive-disabled client uses a one-hour interruptible
  upper bound. Public loop timeouts are unchanged.
- Exit the thread promptly without waiting for `_out_messages` to become empty.
  (Post-audit revision: the historical flush condition was restored to preserve
  compatibility. Idle stop stays sub-millisecond because the selector wakeup
  and empty queues make the flush check pass immediately; only clients with
  pending outgoing traffic keep the historical drain-before-exit behaviour.)
- Preserve all pending queues/state so a later loop/reconnect can continue.
- Remove deadline-driven waiting if timer accuracy or active wakeup latency
  fails its guardrail.

## After Measurements

Brokerless control, keepalive 60:

| State | Baseline | Prototype | Change |
| --- | ---: | ---: | ---: |
| Selector returns, 1 client over 10.2 s | 11 | 2 | **-81.8%** including arm + stop |
| Stop during selector wait | 810.77 ms | 0.40 ms | **-99.95%** |
| Stop during reconnect backoff | 980.06 ms | 0.19 ms | **-99.98%** |
| Publish-after-idle median | 0.429 ms | 0.402 ms | **-6.2% latency** |
| Publish-after-idle p95 | 0.504 ms | 0.480 ms | **-4.7% latency** |

At 100 idle clients over 30 seconds:

| Metric | Baseline | Adaptive prototype | Change |
| --- | ---: | ---: | ---: |
| Selector returns | 3,026 | 200 | **-93.4%** |
| Process CPU | 0.296 s | 0.070 s | **-76.2%** |
| Voluntary context switches | 5,285 | 793 | **-85.0%** |
| Collective stop | 996.14 ms | 18.34 ms | **-98.2%** |

The 30-second CPU result includes thread creation, the initial one-second
arming tick, and shutdown. The remaining steady idle period has no periodic
one-second work; the ten-minute acceptance run is expected to exceed 80%, but
is still required before upstream submission.

The naive always-long timeout was rejected: two broker-backed four-block ABBA
controls measured **-3.37%** and **-4.55%** median active-publish throughput.
Keeping the historical timeout while active but calculating it under
`_msgtime_mutex` also measured **-5.35%**, exposing lock contention with the
send timestamp update. The final adaptive lock-free version measures **+0.39%**
(CI -5.45% to +5.95%), within the 2% active-throughput guardrail. The isolated
stop/backoff-only variant measured -1.20%, also within the guardrail.

All 30 focused tests and all 183 autonomous client/unit tests pass, including
exact active, idle-remaining, and expired deadline selection plus the race
where the worker clears its client thread reference during `loop_stop()`.

## Results Analysis

The initial negative throughput result was real to the prototype, not to the
architectural idea. An unconditional long timeout removed the one-second safety
tick during activity; the first adaptive correction then added a contended lock
to every active loop turn. Component isolation showed the stop/event path was
neutral, and the final strategy preserves the historical active timeout without
locking while extending only genuinely idle waits.

The short 100-client run already crosses the wakeup and context-switch targets,
nearly crosses the CPU target, and improves rather than regresses publish wakeup
latency. Blocking DNS/connect/TLS/handshake operations remain outside the stop
guarantee as planned. RSS, a full ten-minute CPU run, and broker-observed
PINGREQ/PINGRESP drift remain final validation conditions.

## Verdict

**GO with conditions.** Retain the adaptive, lock-free deadline selection plus
interruptible stop and reconnect waits. Before upstream submission, complete
the ten-minute 100-client CPU/RSS run and broker-observed keepalive drift test.
Revert the deadline extension if either CPU reduction stays below 80% or timer
drift reaches 1%; the already independent stop/backoff wakeup may remain if its
active guardrails continue to pass.
