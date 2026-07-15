# 28 - Cold Start and Imports

## Analysis

Importing `paho.mqtt.client` eagerly imports `urllib.parse` and
`urllib.request`, although they are used only by private proxy discovery.
Typical installations without PySocks return from that proxy path immediately,
so importing the relatively large `urllib.request` graph at module load cannot
benefit their import or first connection.

Python 3.9 also makes several compatibility fallbacks obsolete. Direct
`time.monotonic`, SSL-context facilities, `Literal`, `Protocol`, `TypedDict`,
and built-in generic types simplify the module. These maintenance changes must
remain separate from the measured lazy-import change so they cannot obscure
its result. `Required` and `NotRequired` are newer than Python 3.9 and need an
appropriate typing-only treatment if touched.

## Preparation

- Add a fresh-process benchmark reporting import wall time, process CPU, RSS,
  and imported-module count for `paho.mqtt.client`.
- Measure import-only, helper startup without PySocks, normal first connection,
  and proxy discovery with an explicit proxy, environment proxy, `no_proxy`,
  and platform proxy behavior.
- Capture `-X importtime` evidence without using it as the final wall-clock
  metric.
- Use two warmups and seven runs while tuning, then at least fifteen fresh
  processes per paired final result with stable CPU affinity.
- Keep `ssl` eager in the initial candidate; its central transport role and
  smaller measured cost do not justify broader lazy-loading complexity.

## Expected Gain

Priority: P2.

A fresh interpreter currently imports `paho.mqtt.client` in about 98 ms.
Exploratory import-time attribution assigns about 23 ms to `urllib.request`,
5.6 ms to `ssl`, and 3.1 ms to `typing`. Deferring `urllib` should materially
reduce cold-start CPU, latency, imported modules, and initial RSS for ordinary
non-proxy users. It does not affect MQTT packet throughput or network bytes.

Typing and compatibility cleanup is expected to improve maintainability, not
runtime performance, and receives no performance claim.

## Acceptance Criteria

- Reduce median cold import and no-PySocks helper startup time by at least 15
  percent in fresh-process paired measurements.
- Do not regress normal connection setup by more than 2 percent.
- Preserve explicit proxy, proxy environment variables, `no_proxy`, system
  proxy discovery, proxy authentication, and optional-dependency behavior.
- Do not make `ssl` lazy in the first candidate.
- Commit measured lazy imports separately from typing and compatibility-only
  cleanup.

## Before Measurement

The final baseline is plan 27 commit `f832437`. The fresh-process harness loads
baseline and candidate from separate source roots, pins every child to CPU 2,
and records the timed import inside the process rather than including
interpreter startup. Each mode used two warmups and fifteen A-B-B-A blocks,
giving 30 measured processes per version.

PySocks is installed on the measurement host. The two primary modes therefore
place `None` in `sys.modules["socks"]` before importing Paho, reproducing an
installation without the optional dependency. A third mode retains real
PySocks and times client import, construction, and the first `_get_proxy()`
call so deferred work cannot disappear from the accounting.

Earlier exploratory attribution was:

| Cold-start component | Indicative time |
| --- | ---: |
| Import `paho.mqtt.client` | 97.8 ms |
| `urllib.request` import graph | 23.0 ms |
| `ssl` | 5.6 ms |
| `typing` | 3.1 ms |

These figures are not additive and are not used as final evidence. The final
baseline for client import without PySocks is 41.563 ms wall / 41.567 ms CPU,
63 new modules, and 23,380 KiB process peak RSS. The publish helper baseline is
41.923 ms wall / 41.921 ms CPU, 64 modules, and 23,528 KiB RSS.

## Implementation

Removed module-level `urllib.parse` and `urllib.request` imports. `_get_proxy()`
now imports them only after both fast paths that do not need environment
discovery: PySocks is unavailable, or the client has a valid explicit proxy.
Environment proxy, `no_proxy`, and PySocks default-proxy discovery execute the
same standard-library calls as before and pay the import once through Python's
module cache.

Added `cold_start_import_eval.py`. It uses fresh worker processes, separate
source roots, A-B-B-A ordering, simulated no-PySocks installations, and a
real-PySocks first-lookup guardrail. It records wall/CPU time, module count,
peak RSS, affinity, and whether `urllib.request` was loaded.

Added tests proving that no `urllib` import occurs on the no-PySocks and
explicit-proxy paths, and that HTTP environment proxy, `no_proxy`, and PySocks
default-proxy results remain unchanged.

`ssl` remains eager. Obsolete Python compatibility branches and typing aliases
are deliberately not part of this measured diff. A separate maintenance
follow-up directly imports the Python 3.9 typing primitives, uses built-in
generic containers, selects `time.monotonic` directly, and removes TLS
capability tests that are guaranteed on the supported floor. None of the
cold-start gain above is attributed to that follow-up.

## After Measurements

Final medians from 30 fresh processes per version:

| Mode | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| Client import, no PySocks, wall | 41.563 ms | 28.963 ms | **-30.31%** |
| Client import, no PySocks, CPU | 41.567 ms | 28.968 ms | **-30.31%** |
| Client import, no PySocks, modules | 63 | 38 | **-39.68%** |
| Client import, no PySocks, peak RSS | 23,380 KiB | 21,972 KiB | **-6.02%** |
| Publish helper, no PySocks, wall | 41.923 ms | 30.256 ms | **-27.83%** |
| Publish helper, no PySocks, CPU | 41.921 ms | 30.256 ms | **-27.83%** |
| Publish helper, no PySocks, modules | 64 | 39 | **-39.06%** |
| Publish helper, no PySocks, peak RSS | 23,528 KiB | 21,994 KiB | **-6.52%** |

Neither no-PySocks candidate loaded `urllib.request`; every baseline did.

The real-PySocks import plus first environment/default-proxy lookup keeps the
deferred work in scope:

| First proxy lookup included | Baseline | Candidate | Delta |
| --- | ---: | ---: | ---: |
| Wall | 42.664 ms | 42.981 ms | +0.74% |
| CPU | 42.658 ms | 42.984 ms | +0.76% |
| New modules | 64 | 64 | unchanged |
| Peak RSS | 23,508 KiB | 23,500 KiB | -0.03% |

Correctness validation under Python 3.12.3:

```text
58 targeted tests passed
281 main tests passed, 21 skipped
```

## Results Analysis

The no-PySocks cold-start gain is substantially larger than the 15 percent
threshold and appears consistently in wall time, CPU, module count, and RSS.
This benefits short-lived helpers, command-line tools, serverless-style
workers, test discovery, and applications that import many optional clients.
It does not change MQTT packet throughput or network traffic.

The optimization removes work only when it is unnecessary. With PySocks and
environment discovery, import plus first lookup is effectively neutral at
+0.74 percent and remains below the 2 percent guardrail. Later lookups use the
normal Python module cache. Explicit proxy users also retain a fast path and do
not need `urllib` discovery at all.

Peak RSS is `ru_maxrss` for the complete fresh worker, not an allocation claim
for Paho alone. The 1.4--1.5 MiB median reduction is therefore reported as a
process-level observation supported by the 25 avoided modules, not as exact
retained Python-object accounting.

Proxy behavior is unchanged because the same `urllib.request.proxy_bypass`,
`getproxies`, and `urllib.parse.urlparse` functions are called. The new tests
cover explicit, environment, bypass, and PySocks-default paths. Platform system
proxy resolution remains delegated to the standard library exactly as before.

Making `ssl` lazy would touch central transport types and setup paths for a
much smaller attributed cost and remains rejected. Typing and compatibility
cleanup has maintenance value but must not inherit this import-performance
claim without its own measurement.

## Verdict

**GO.** Retain the lazy `urllib` imports. Client and publish-helper cold starts
without PySocks improve by 28--30 percent, process peak RSS falls by about 6
percent, and the first real proxy-discovery path remains neutral within the 2
percent guardrail. All targeted and main tests pass.

No MQTT throughput gain is claimed. The obsolete typing and runtime fallback
cleanup is kept in a separate maintenance commit, and `ssl` must not become
lazy without a new profile and plan.
