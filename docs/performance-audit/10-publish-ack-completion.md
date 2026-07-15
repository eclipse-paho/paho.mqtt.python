# 10 - Publish ACK Completion Fast Path

## Analysis

MQTT v3 PUBACK/PUBCOMP handling constructed `ReasonCode` and `Properties` objects even when no `on_publish` callback was registered. These objects are callback metadata and are not needed to complete the outgoing QoS state machine.

## Preparation

The saturated QoS 1 harness parses PUBACK, completes `MQTTMessageInfo`, removes the message, and promotes the next queued message. Legacy and candidate handlers are compared in the same process over 500 ACKs and 21 runs.

## Expected Gain

Priority: P1. Target at least 8% higher MQTT v3 ACK throughput without changing callback or MQTT v5 behavior.

## Acceptance Criteria

- At least +8% for MQTT v3 QoS 1 without `on_publish`.
- No callback metadata allocation on that path.
- Unknown or duplicate ACK remains harmless.
- MQTT v3 with callback and all MQTT v5 ACK validation remain unchanged.

## Before Measurement

Legacy handler: **35,424 ACK/s** for the saturated 500-ACK comparison.

## Implementation

- Split `_complete_outgoing_publish()` from callback dispatch.
- Added an MQTT v3 fast path that snapshots `_on_publish` and completes directly when it is absent.
- Kept MQTT v5 reason-code and property parsing unconditional for protocol validation.
- Replaced the two-byte ACK slice/unpack with the shared `Struct.unpack_from()`.

## After Measurements

Candidate handler: **41,954 ACK/s**, or **+18.4%**. The standalone post-change no-promotion scenario reaches about 84k ACK/s.

## Results Analysis

The gain comes from avoiding two objects and a callback-dispatch layer, not from changing inflight promotion. This does not reopen the rejected ready-queue design.

## Verdict

**GO.** The measured gain exceeds the P1 threshold and focused tests cover missing callbacks and unknown ACKs.
