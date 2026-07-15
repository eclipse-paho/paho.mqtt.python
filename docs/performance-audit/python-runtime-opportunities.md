# Python Runtime Opportunities

This backlog separates runtime-floor maintenance from optimization projects.
It prevents attractive language features from being presented as performance
work without an application profile and measurable acceptance threshold.

## Current Python 3.9 Floor

The audit, package metadata, README, classifiers, and test matrix use Python
3.9 as the real minimum. The alignment is kept in a commit separate from plans
26-28 so benchmark baselines and compatibility effects remain attributable.

Python 3.9 guarantees insertion-ordered dictionaries, native generic aliases
such as `list[str]`, the typing primitives currently hidden behind older
fallbacks, and `socket.socketpair()` on all supported platforms. The first two
are primarily opportunities to remove compatibility code; the mapping and
socket-pair projects have their own measurable performance plans.

The initial Python 3.9 cleanup is complete:

- `Literal`, `Protocol`, and `TypedDict` come directly from `typing`;
- callback aliases use built-in `dict`, `list`, and `tuple` generics;
- `Iterator` and `Sequence` come from `collections.abc`;
- the client selects `time.monotonic` directly;
- TLS uses the guaranteed `SSLContext`, `load_default_certs`,
  `PROTOCOL_TLS_CLIENT`, and `check_hostname` facilities without historical
  capability branches.

This is maintenance, not a performance result. The optional `ssl` import
guard remains for Python builds without OpenSSL. The wrap-without-SNI retry and
legacy hostname-verification fallback remain because they concern runtime TLS
contexts and builds rather than the declared Python version. `Union` remains
where a type alias is evaluated at runtime: replacing it with `|` would require
careful Python 3.9 runtime evaluation checks, not a mechanical syntax rewrite.
The `Required` / `NotRequired` fallback in `publish.py` also remains because
those two primitives are not available from `typing` until Python 3.11;
`Literal` and `TypedDict` no longer share that fallback.

`asyncio.to_thread()` does not justify a new asynchronous public API. It runs a
blocking call in a worker thread and does not replace Paho's existing external
event-loop integration or solve callback scheduling by itself. Reconsider it
only for a concrete application profile dominated by unavoidable blocking I/O.

## Future Python 3.10 and Later

### Python 3.10: Linux `eventfd`

`os.eventfd()` could replace a two-socket wakeup channel with one descriptor on
Linux. Do not prototype it until a profile with at least 1,000 live clients
shows that descriptor count, wakeup syscalls, or socket-pair memory is material
after plan 27. Any future design must retain a portable socket-pair path and
therefore justify the extra platform branch.

### Python 3.13: free-threaded CPython

Treat free-threading as a correctness audit before treating it as a throughput
opportunity. Inventory shared message maps, socket lifecycle, callback
replacement, reconnect state, locks, and helper globals; add a `3.13t` test
lane and race-oriented stress tests. Claim parallel gains only after that lane
is stable and a callback- or codec-heavy workload benefits without weakening
ordering or QoS behavior.

### Syntax and library modernization

Dictionary unions, `removeprefix`, newer union syntax, newer typing syntax, and
modern dataclass options can simplify source code but are not performance
projects. Apply them only when they improve a touched area and keep them out of
benchmark-attributed commits.

## Closed Without a New Profile

- Do not add subinterpreters merely to parallelize callbacks; ownership,
  serialization, extension compatibility, and ordering costs are unproven.
- Do not add a general callback executor. It creates backpressure, shutdown,
  exception, and ordering policy that the current synchronous callback API
  does not expose.
- Do not create a second asyncio facade around the threaded client. Existing
  socket callbacks support native event-loop integration without another
  queue and thread boundary.
- Do not introduce a platform-specific wakeup path or new public execution
  mode solely because a newer runtime provides a primitive.

Every future item must first receive an isolated plan with the standard nine
sections, a realistic workload, explicit latency and memory guardrails, and a
rollback path.
