# 13 - Reconnect Reset and Replay

## Analysis

Reconnect scans are necessarily O(N), but the loop recomputed clean-session state for every QoS 2 message, called the clock for every replayed message, and decoded/re-encoded topics whose original bytes are already stored internally.

## Preparation

Reset benchmarks use 100, 1,000, and 10,000 QoS 2 messages with persistent-session state. Tests assert states, DUP flags, order, and use of the internal topic bytes.

## Expected Gain

Priority: P2. Target at least 10% on a 1,000-message reconnect reset without adding a second queue or changing the MQTT state machine.

## Acceptance Criteria

- At least +10% for 1,000 QoS 2 messages.
- State, order, DUP, inflight accounting, and callback behavior remain unchanged.
- No ready-queue or public API is introduced.

## Before Measurement

| Messages | Before reset/s |
| ---: | ---: |
| 100 | 31,640 |
| 1,000 | 3,101 |
| 10,000 | 240 |

## Implementation

- Compute clean-session state once per reset pass.
- Compute one reconnect timestamp per CONNACK replay pass.
- Use `MQTTMessage._topic` bytes in replay and inflight promotion instead of `message.topic.encode()`.
- Preserve the existing authoritative `OrderedDict` and state transitions.

## After Measurements

| Messages | After reset/s | Delta |
| ---: | ---: | ---: |
| 100 | 40,498 | **+28.0%** |
| 1,000 | 3,829 | **+23.5%** |
| 10,000 | 328 | **+36.4%** |

The permanent harness scenario reports about 4,034 reset/s for 1,000 messages.

## Results Analysis

The scan remains O(N), as required to update every message, but repeated invariant work is removed. Reusing `_topic` also avoids a decode/encode cycle during ordinary queued-message promotion.

## Verdict

**GO.** The change exceeds the threshold without additional state structures.
