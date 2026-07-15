# 27 - Native Socket Pair

## Analysis

The threaded-loop wakeup channel is created by a private compatibility helper
that binds a TCP listener on loopback, connects a client socket, accepts the
connection, and closes the listener. This emulates `socket.socketpair()` at
the cost of several syscalls, an ephemeral TCP port, and extra failure paths.

`socket.socketpair()` is available on Windows since Python 3.5 and is therefore
guaranteed on every platform supported by the new Python 3.9 floor. The
existing helper can remain as the private abstraction while its implementation
becomes a native pair with both endpoints set non-blocking.

## Preparation

- Extend wakeup tests to cover repeated creation/destruction, `loop_start()` /
  `loop_stop()`, pair replacement while a wakeup is pending, coalesced writes,
  concurrent restart, and file-descriptor cleanup.
- Add a benchmark for pair creation, first-wakeup latency, and preparation and
  teardown of 1, 100, and 1,000 clients.
- Record wall and CPU time, p50/p95/p99, socket-related syscall counts where
  available, descriptor counts, and failures.
- Use two warmups and seven runs during tuning, then fifteen paired final runs;
  pin sub-millisecond measurements to one CPU.
- Validate on Linux locally and make Python 3.9 Windows a mandatory final
  portability gate.

## Expected Gain

Priority: P2.

The exploratory helper-only median is 67.7 microseconds for the loopback TCP
emulation and 9.1 microseconds for `socket.socketpair()`, a 7.4-fold speedup or
about 86.6 percent less creation time. The improvement affects client
preparation, threaded-loop restart, descriptor pressure, and reliability; it
does not claim a steady-state MQTT throughput or wire-traffic gain.

## Acceptance Criteria

- Reduce pair-creation median by at least 50 percent.
- Reduce the preparation phase for many clients by at least 10 percent.
- Do not regress first-wakeup or `loop_start()` / `loop_stop()` latency by more
  than 2 percent.
- Preserve non-blocking behavior, wakeup coalescing, callback ordering,
  concurrent restart, and descriptor cleanup with no lost wakeup or deadlock.
- Pass the focused suite on Python 3.9 for Windows before an unconditional
  final verdict; a local-only result must remain `GO with conditions`.

## Before Measurement

The final baseline is plan 26 commit `7ee1418`. Baseline and candidate were
loaded from separate source trees under CPython 3.12.3, pinned to CPU 2. The
final A-B-B-A comparison used two worker processes per version, each with two
warmups and fifteen runs. Every run created 1,000 pairs, prepared and destroyed
1,000 clients with a pair, and completed 1,000 send/select/receive wakeups.

The earlier isolated probe was:

| Pair implementation | Median creation time |
| --- | ---: |
| Loopback TCP compatibility helper | 67.7 us |
| Native `socket.socketpair()` | 9.1 us |

That probe was motivational only. The final baseline measured 91.43 us per
pair, 118.23 us per prepared client, a 14.83-us median wakeup, and no residual
file descriptors.

## Implementation

Kept `_socketpair_compat()` as the single private abstraction, but replaced its
loopback TCP listener, bind, listen, non-blocking connect, accept, and listener
close sequence with `socket.socketpair()`. Both returned endpoints are still
made non-blocking explicitly.

Added `socketpair_lifecycle_eval.py`, which loads baseline and candidate from
separate source roots and measures them in A-B-B-A order. It reports pair and
client lifecycle wall/CPU time, first-wakeup median and p95, endpoint metadata,
and descriptor deltas.

Added tests for concrete sockets, duplex communication, non-blocking empty
reads, twenty consecutive threaded-loop start/stop cycles, old-pair closure,
and final cleanup. Existing tests continue to cover coalescing, concurrent
packet queueing during pair replacement, stop wakeups, and worker-reference
races.

No public option, callback change, platform branch, or MQTT state change was
introduced.

## After Measurements

Final medians across the two A-B-B-A workers per version:

| Metric | Loopback TCP baseline | Native candidate | Delta |
| --- | ---: | ---: | ---: |
| Pair creation wall | 91.430 us | 13.962 us | **-84.73%** |
| Pair creation CPU | 91.425 us | 13.964 us | **-84.73%** |
| Prepared client wall | 118.226 us | 31.010 us | **-73.77%** |
| Prepared client CPU | 118.223 us | 31.011 us | **-73.77%** |
| Wakeup median | 14.830 us | 5.044 us | **-65.98%** |
| Wakeup p95 | 25.117 us | 5.250 us | **-79.10%** |
| Wakeup CPU | 16.466 us | 5.632 us | **-65.79%** |
| Residual descriptors | 0, 0 | 0, 0 | unchanged |

On Linux the private endpoints change from `AF_INET/SOCK_STREAM` to
`AF_UNIX/SOCK_STREAM`; both remain non-blocking, connected, selectable stream
sockets. No endpoint is exposed through Paho's socket callbacks.

A short `strace -c -e trace=network` control over 100 pair creations counted
900 network-class syscalls for the compatibility implementation and 300 for
the native implementation. The latter uses 100 `socketpair` calls plus the
socket-object metadata queries; it eliminates 100 each of `connect`, `accept`,
`bind`, `listen`, and `setsockopt`, as well as the temporary listener lifecycle.
Close and non-network-class syscalls are outside this filtered count.

Correctness validation under Linux and Python 3.12.3:

```text
55 targeted tests passed
278 main tests passed, 21 skipped
```

## Results Analysis

This is primarily runtime-floor cleanup, not a packet-path throughput project.
Python already provides the portable abstraction, so maintaining a second
TCP-based implementation adds code and operating-system interactions without
providing compatibility on the supported runtime range.

The measured lifecycle gains are nevertheless real and comfortably exceed the
informational thresholds. Creating many threaded clients is about 74 percent
cheaper in this isolated preparation phase, and the native Unix-domain wakeup
is faster locally. These numbers must not be translated into MQTT messages per
second: an established client creates the pair only at loop start/restart, and
wakeup coalescing already avoids one wakeup per published message.

The more general benefits are eliminating ephemeral loopback ports, a
temporary listening descriptor, network-stack setup, firewall interactions,
and several exceptional cleanup paths. Active clients still retain two socket
endpoints, so no steady-state descriptor-count reduction is claimed. Network
wire traffic for MQTT is unchanged.

Linux correctness and descriptor cleanup are established. Python documents
`socket.socketpair()` support on Windows since Python 3.5, but this environment
cannot execute the required Python 3.9 Windows test. That portability gate
therefore remains explicit instead of being inferred from the Linux result.

## Verdict

**GO with conditions.** Retain this as a Python 3.9 modernization and code
cleanup with beneficial lifecycle measurements. Pair creation, many-client
preparation, and wakeup latency all improve substantially; duplex,
non-blocking, restart, concurrency, and descriptor-cleanup behavior pass.

Do not advertise an MQTT throughput gain. Before making the verdict
unconditional, run the focused lifecycle and wakeup tests on Windows with
Python 3.9. Revert or add only the narrowest necessary fallback if that real
platform test contradicts Python's documented support; do not restore the
emulation preemptively.
