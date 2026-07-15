# 08 - Logging and Observability

## Problem

Performance work needs reliable measurement and low-noise observability. The
relevant runtime path is `Client._easy_log()`, and the broader project need is a
repeatable benchmark/profiling workflow.

Likely symptoms:

- Debug logging can format messages and property strings on hot paths.
- Profiles may over-represent logging or callback cost if logging is enabled.
- No current benchmark suite exists to prevent performance regressions.
- Performance claims can become anecdotal without consistent scenarios.

Common workloads:

- Contributors testing optimizations locally.
- Users reporting high CPU with production profiles.
- Maintainers reviewing pull requests that alter packet parsing or queue logic.

## Theoretical Rationale

Performance regressions are often introduced by small changes in hot paths:
extra allocations, extra string formatting, extra lock acquisitions, or extra
syscalls. Without benchmark guardrails, those changes are hard to detect in a
functional test suite.

Logging has two different costs:

- Disabled logging should be nearly free.
- Enabled debug logging may be expensive, but the cost should be explicit and
  avoidable.

The audit should separate library cost from user callback and logging cost.

## Expected Gain

Priority: P1.

Conservative expected gain:

- Better confidence rather than immediate runtime speed.
- Potential 1 to 5 percent hot-path improvement if avoidable log formatting or
  property stringification is found.
- Faster review of future performance pull requests.

This project is an enabler for all other projects and should be started early
even if runtime changes are small.

## Before/After Measurements

Microbenchmarks:

- `_easy_log()` with no logger and no callback.
- `_easy_log()` with logger configured above debug level.
- `_easy_log()` with debug logger enabled.
- `_send_publish()` and `_handle_publish()` with logging disabled and enabled.

Benchmark harness:

- Add or document brokerless benchmark commands for parser, packer, properties,
  matcher, inflight, and WebSocket scenarios.
- Add optional broker scenario documentation using a local broker.
- Record environment metadata: Python version, OS, CPU model if available,
  transport, QoS, payload size, protocol version.

Profiling workflow:

- Use `cProfile` for deterministic call counts.
- Use `tracemalloc` for allocations.
- Use `py-spy` when installed for low-overhead sampling.
- Use Linux `perf` when available for syscall and scheduler context.

## Implementation Guidelines

Allowed implementation directions:

- Keep benchmark tooling optional and dependency-free by default.
- Prefer scripts or pytest-marked benchmarks that can run without a broker.
- Ensure benchmark code does not affect package runtime imports.
- Guard expensive log argument construction if profiling shows it occurs while
  logging is disabled.
- Avoid changing user-visible log message content unless necessary.

Risks:

- Benchmarks can become flaky if they depend on external brokers or network
  timing.
- Overfitting to microbenchmarks can harm real-world workloads.
- Logging changes can break users who assert exact log messages.

## Acceptance Criteria

Functional criteria:

- Existing tests pass.
- Benchmark documentation explains how to run brokerless scenarios.
- Optional broker scenarios are clearly marked optional.
- Logging behavior remains compatible for callbacks and standard loggers.

Performance criteria:

- Disabled logging path has no measurable regression.
- Any logging hot-path change must show at least 5 percent improvement in the
  targeted logging microbenchmark or be rejected.
- Benchmark runs report medians across at least 5 iterations.

Documentation criteria:

- Add a result template for future optimization pull requests.
- Document minimum metadata required for performance reports.
- Link each project file to the common measurement method in `README.md`.

## Verdict

GO.

Justification: benchmark and profiling discipline is required before accepting
or rejecting the other optimization projects. Runtime logging changes should be
small and evidence-driven, but the observability plan should proceed.

## Progress (2026-07-09)

Status: **Done for harness / measurement workflow**. Runtime `_easy_log`
micro-optimizations not pursued (disabled path already cheap enough).

Commit: `238eee8` (`perf: add standalone benchmark harness and audit plans`).

### Implemented

- Brokerless harness under `benchmarks/` (`run.py`, `compare.py`, `scenarios.py`,
  `fakes.py`, `README.md`).
- Scenarios covering properties, reason codes, publish parse/pack, packet drain,
  sockpair coalesce, threaded publish, matcher, disabled logging.
- `compare.py` gain / regression thresholds aligned with audit practice.
- This `docs/performance-audit/` plan set.

### Residual (optional)

- Result template checked into docs for PR authors.
- Optional local-broker scenario scripts (explicitly optional).

### Follow-up (2026-07-09)

`_handle_publish` now skips UTF-8 topic decode / DEBUG format when neither
`on_log` nor `_logger` is set (~+9.5% parse QoS0, same-process). Documented in
`01-packet-read-parser.md`.
