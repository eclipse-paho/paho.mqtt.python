# 25 - TLS Session Resumption

## Analysis

Every reconnect currently creates a new TCP socket and performs a complete TLS
handshake. Python exposes the verified `SSLSession` through `SSLSocket.session`
and accepts it on the next `SSLContext.wrap_socket(session=...)`. This can
remove public-key work and round trips without changing MQTT session state.

The optimization is useful only for clients which reconnect to the same
endpoint with the same `SSLContext`. It must therefore remain private, retain
at most one session per client, and key it by context object, hostname, port,
and transport.

The first prototype exposed an important transport interaction. A resumed TLS
1.2 handshake ends with a client flight. With the client's current Nagle
policy, the first MQTT `CONNECT` or WebSocket upgrade write can then wait for a
delayed ACK. On local Linux this changed a roughly 3--5 ms full connection into
a repeatable 42--45 ms resumed connection even though handshake CPU fell.
`TCP_NODELAY` removes that plateau, but enabling it globally would be a separate
network-policy change with a packet-count tradeoff. The refined proposal never
changes that option: TLS 1.3 remains unconditional, while a TLS 1.2 session is
cached and presented only when the raw socket already reports
`TCP_NODELAY=1` before wrapping.

## Preparation

- Added `benchmarks/tls_session_eval.py`, a verified local TLS server/client
  probe independent from a broker.
- Corrected the probe so the client sends the first application byte, matching
  MQTT and WebSocket connection ordering. The earlier server-first probe
  measured cryptographic handshake cost but missed the TLS 1.2 delayed-ACK
  regression.
- Added raw MQTT/TLS and WSS transports, fixed TLS 1.2 and TLS 1.3 versions,
  server-context rotation, optional `TCP_NODELAY`, phase timings, warmups, and
  multi-run aggregation. Reports now record CPU affinity because client and
  server threads share one process: unpinned `process_time()` and handshake
  medians showed large scheduling/frequency drift that disappeared under a
  one-CPU ABBA guardrail.
- Used a fresh local certificate with `localhost` SAN and normal CA/hostname
  verification because the repository certificate fixture is expired at the
  measurement date. No verification was disabled.
- Used CPython 3.12.3 and OpenSSL 3.0.13. Python 3.9 is the actual compatibility
  floor, but no Python 3.9 interpreter is installed in this environment.
- Final accepted-path measurements use 2 warmups, 15 runs, and 20 reconnects
  per run. Short ABBA probes were used only to diagnose TLS 1.2 variance.

## Expected Gain

Priority: P2.

- At least 25 percent lower median TLS 1.3 reconnect latency.
- At least 20 percent lower handshake CPU.
- A bounded one-session memory cost per `Client`.
- No repeated penalty when a server refuses a cached session.
- TLS 1.2 gains only when the socket already opts out of Nagle, with no session
  extraction or presentation on the default path.

## Acceptance Criteria

- Cache a session only after a verified TLS 1.3 handshake, or after a verified
  TLS 1.2 handshake whose raw socket already has `TCP_NODELAY=1`.
- Present a cached TLS 1.2 session only when the newly created raw socket also
  reports `TCP_NODELAY=1`; inability to query the option means no reuse.
- Reuse it only with the identical `SSLContext` object, hostname, port, and
  transport.
- Clear it on target/context change, reinitialise, certificate error, or local
  incompatible-session error.
- If the server declines a presented session, perform the valid full handshake
  and stop presenting sessions for that exact target until its identity
  changes.
- Never change certificate validation, hostname checking, SNI, ALPN, client
  certificate, cipher, proxy, MQTT session, or callback behavior.
- At least 25 percent lower median reconnect time or 20 percent lower CPU.
- No full-handshake fallback regression above 3 percent.
- Raw MQTT/TLS and WSS both remain correct.
- Production code remains valid on the actual Python 3.9 floor. A Python 3.9
  execution and a real-broker reconnect run are mandatory follow-ups before
  considering the result unconditional.

## Before Measurement

The baseline is commit `a1a7a2a`. Final TLS 1.3 rows are the median of 15 run
medians; every run contains 20 reconnects after its initial connection.

| Transport | TLS | Median wall | Median CPU | p95 wall | Reused |
| --- | --- | ---: | ---: | ---: | ---: |
| MQTT/TCP | 1.3 | 3.293 ms | 3.596 ms | 4.211 ms | 0/300 |
| WSS | 1.3 | 4.736 ms | 5.254 ms | 5.798 ms | 0/300 |

Corrected client-first tuning probes established the TLS 1.2 hazard before it
could enter production:

| Transport | Full handshake | Resumed handshake | Result |
| --- | ---: | ---: | --- |
| MQTT/TCP TLS 1.2 | 3.524 ms | 43.072 ms | unacceptable delayed-ACK plateau |
| WSS TLS 1.2 | 4.838 ms | 43.609 ms | unacceptable delayed-ACK plateau |

Enabling `TCP_NODELAY` in the probe reduced resumed WSS TLS 1.2 to 1.743 ms,
confirming the cause. That result is diagnostic only: the production client
does not silently change its TCP policy as part of this plan.

Encrypted wire bytes were not attributed reliably by the local probe and are
not claimed as a result. Python allocation counts also omit the OpenSSL-owned
session allocation; the implementation retains exactly one session reference
and bounded identity metadata per client. Long-lived multi-client RSS remains
a system-level follow-up rather than an acceptance claim.

## Implementation

- Store one private `SSLSession` and its exact target/context identity.
- Query `TCP_NODELAY` passively on the raw socket before wrapping. Attribute and
  socket-option errors conservatively mean false; the client never sets the
  option.
- Mark a socket as cacheable only after TLS verification has completed and
  `SSLSocket.version()` reports TLS 1.3, or TLS 1.2 with the pre-wrap option
  already enabled. The negotiated identity and protocol are captured at that
  point rather than reconstructed later at close.
- At orderly or error-driven socket close, extract an eligible session from
  the raw SSL socket or the socket under `_WebsocketWrapper`.
- Pass a matching session to `SSLContext.wrap_socket()` on reconnect.
- If OpenSSL rejects the session locally with `ValueError`, close that TCP
  socket and retry once on a fresh TCP connection without the session.
- If the peer completes a full handshake instead of resuming, clear the cache
  and disable further attempts for that unchanged target. This bounds the
  refusal cost to one reconnect and avoids a permanent rejected-ticket tax.
- Leave TLS 1.2 on the original full-handshake path whenever the new socket
  does not already have `TCP_NODELAY`. No TCP setting, public option, callback,
  or MQTT state was added.
- Added unit tests for matching and changed targets, WSS extraction, TLS 1.2
  exclusion, insecure mode, certificate errors, local mismatch retry, server
  refusal, and fresh-socket retry.

## After Measurements

Final TLS 1.3 results, with the same 2-warmup/15-run/20-reconnect protocol:

| Transport | Median wall | Delta | Median CPU | Delta | p95 wall | Reused |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| MQTT/TCP | 1.936 ms | **-41.2%** | 2.230 ms | **-38.0%** | 2.458 ms (-41.6%) | 300/300 |
| WSS | 2.460 ms | **-48.0%** | 2.787 ms | **-47.0%** | 2.756 ms (-52.5%) | 300/300 |

With a new server TLS context for every connection, every offered ticket was
refused. Over 15 final runs the baseline/candidate results were:

| TLS 1.3 rejection | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| Median wall | 4.688 ms | 4.701 ms | +0.29% |
| Median CPU | 5.006 ms | 5.136 ms | +2.60% |
| Reused | 0/300 | 0/300 | expected |

The conditional TLS 1.2 refinement was finalized with 2 warmups, 15 runs, and
20 reconnects per run. The no-reuse guardrail uses an A-B-B-A order pinned to
one CPU because unpinned OpenSSL/process medians varied by more than the entire
expected guardrail despite overlapping ranges.

| TLS 1.2 path | Baseline wall | Candidate wall | Wall delta | CPU delta | Reused |
| --- | ---: | ---: | ---: | ---: | ---: |
| MQTT/TCP, Nagle retained (pinned ABBA) | 2.639 ms | 2.573 ms | **-2.53%** | **-2.55%** | 0/600 |
| WSS, Nagle retained (pinned ABBA) | 3.006 ms | 2.964 ms | **-1.41%** | **-1.51%** | 0/600 |
| MQTT/TCP, `TCP_NODELAY` | 4.360 ms | 1.348 ms | **-69.1%** | **-66.6%** | 300/300 |
| WSS, `TCP_NODELAY` | 4.148 ms | 1.631 ms | **-60.7%** | **-57.6%** | 300/300 |

On the default path the candidate neither caches nor presents a TLS 1.2
session. Pinned close phases remain in the same 0.036--0.043 ms band, so no
cache-extraction tax remains. Short TLS 1.3 controls still resumed 140/140
sessions and remained above the acceptance threshold on both transports after
adding the socket-option query.

Targeted validation passes under Python 3.12:

```text
105 passed in 3.73s
```

This covers `tests/test_tls_session.py`, `tests/test_websockets.py`, and
`tests/test_client.py` with the repository source forced through
`PYTHONPATH=src`.

The full Python 3.12 collection reaches 304 passed, 21 skipped, and 1 expected
failure. Its four remaining TLS integration cases fail because the checked-in
certificate fixtures are expired at the 2026 measurement date; both client and
server report certificate expiration. The plan's real TLS probe uses a fresh
verified certificate and all targeted TLS tests pass, so the fixture failure is
recorded rather than bypassed by disabling verification.

## Results Analysis

TLS 1.3 comfortably exceeds both latency and CPU thresholds on raw TLS and
WSS. The gain includes the first application exchange, so it is not merely a
reduction in an internal OpenSSL timer. Success was deterministic in the local
compatible-server run: 600 of 600 measured reconnects resumed across both
transports.

The server-refusal circuit breaker meets the 3 percent guardrail while keeping
certificate and hostname verification untouched. Its deliberate tradeoff is
performance-only: after one refusal, that `Client` no longer probes the same
target for a newly usable ticket. Reconnection correctness still uses the full
handshake.

Unconditional TLS 1.2 resumption remains rejected: a 40-ms application-latency
regression is more important than reduced handshake work. Conditional reuse is
safe in the local probe because the decisive property is checked on the new
socket, not inferred from the previous connection. It also avoids changing the
packet policy for clients that did not request it.

There is an important reachability limit. `on_socket_open` runs after the TLS
handshake and, for WSS, after the HTTP upgrade, so setting `TCP_NODELAY` in that
callback is too late to enable TLS 1.2 resumption. The conditional optimization
benefits only a socket configured before wrapping, such as one supplied by a
specialized connection path. No callback reordering or new public API is
justified by this plan.

The remaining uncertainty is environmental rather than an invitation to add
more paths: execute the existing code on Python 3.9/OpenSSL and against a real
broker with valid TLS 1.3 tickets. The stale `requires-python >=3.7` package
metadata is outside this optimization commit; the audit's enforced floor is
Python 3.9.

## Verdict

**GO with conditions.** Retain TLS 1.3 session resumption and the passive,
strictly gated TLS 1.2 variant. TLS 1.3 delivers substantial end-to-end gains.
TLS 1.2 also does so when the new raw socket already has `TCP_NODELAY`, while
the default socket retains the full handshake and does not pay session
extraction cost.

Before upgrading this to unconditional `GO`, run the targeted suite and the
raw/WSS reconnect scenario on Python 3.9, then validate reconnect latency,
certificate failure, and RSS against a real TLS 1.3 broker. Validate the
conditional TLS 1.2 path with a raw socket configured before wrapping.
Unconditional TLS 1.2 resumption remains explicitly `NO GO`.
