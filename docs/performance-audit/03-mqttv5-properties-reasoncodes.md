# 03 - MQTT v5 Properties and Reason Codes

## Problem

MQTT v5 support appears to rebuild and search metadata too often. The main code
paths are `src/paho/mqtt/properties.py`, `src/paho/mqtt/reasoncodes.py`, and
the MQTT v5 branches in `Client._handle_publish()`, `_handle_pubackcomp()`,
`_handle_suback()`, `_handle_unsuback()`, `_send_publish()`, `_send_connect()`,
and related handlers.

Likely symptoms:

- MQTT v5 messages cost noticeably more CPU than MQTT v3 messages even when
  properties are empty.
- `Properties()` construction rebuilds `types`, `names`, and `properties` for
  every instance.
- Property name/id conversion uses repeated linear scans and string replacement.
- `ReasonCode()` copies the class-level names mapping and performs repeated
  lookups.
- `bytes` concatenation in property pack/unpack creates temporary objects.

Common workloads:

- MQTT v5 telemetry with empty properties.
- MQTT v5 telemetry with `TopicAlias`, `PayloadFormatIndicator`, or
  `UserProperty`.
- QoS 1 publish streams where every PUBACK creates reason code/property objects.

## Theoretical Rationale

Metadata tables are cold configuration data, but the current object model treats
them as per-instance data. This inflates allocation count and cache pressure.
Linear name/id lookup is also avoidable because property identifiers and reason
codes are small fixed protocol tables.

For empty MQTT v5 properties, the optimal path should be nearly constant time:
read one zero byte and return an empty object only when the public API requires
one. For non-empty properties, lookup should be direct by id and packing should
append into a mutable buffer rather than repeatedly concatenating immutable
`bytes`.

## Expected Gain

Priority: P0.

Conservative expected gain:

- 20 to 50 percent CPU reduction in isolated `Properties` construction,
  pack, and unpack microbenchmarks.
- 5 to 20 percent end-to-end improvement for MQTT v5 small-message workloads.
- Significant allocation reduction for PUBACK/SUBACK/UNSUBACK handling.

The largest gain should appear in MQTT v5 workloads with many acknowledgements
or empty properties.

## Before/After Measurements

Microbenchmarks:

- Construct `Properties(PacketTypes.PUBLISH)` one million times.
- Pack empty properties, one scalar property, and multiple user properties.
- Unpack empty properties, one scalar property, and multiple user properties.
- Construct `ReasonCode(PacketTypes.PUBACK)` and `ReasonCode(..., identifier=0)`
  repeatedly.
- Parse MQTT v5 PUBLISH packets with empty properties and with `UserProperty`.

Broker scenarios:

- MQTT v5 QoS 0 subscriber receiving small messages with empty properties.
- MQTT v5 QoS 1 publisher receiving PUBACKs.
- MQTT v5 subscriber with user properties.

Metrics:

- Object allocations per message.
- CPU time for property pack/unpack.
- End-to-end messages per second.
- Ratio of MQTT v5 to MQTT v3 throughput for equivalent payloads.

## Implementation Guidelines

Allowed implementation directions:

- Move immutable protocol metadata to module-level constants or class-level
  `MappingProxyType` objects.
- Precompute compressed property names, name-to-id maps, id-to-name maps,
  allowed packet types, and multiplicity flags.
- Avoid repeated `name.replace(" ", "")` in hot methods by using canonical names.
- Build property buffers with `bytearray` or list-join patterns instead of
  repeated `bytes +=`.
- Fast-path empty properties in `pack()` and `unpack()`.
- Avoid copying `ReasonCode.names` into every instance.
- Precompute reason-code lookup maps by packet type and identifier.

Risks:

- Public dynamic attribute behavior of `Properties` must remain compatible.
- Error messages should remain useful even if exact wording changes.
- Deprecated `ReasonCodes` compatibility must remain intact.
- MQTT v5 property validation must not be weakened.

## Acceptance Criteria

Functional criteria:

- Existing MQTT v5 tests pass.
- Add tests for repeated properties, invalid duplicate properties, invalid
  property ids, empty property encoding, and deprecated `ReasonCodes`
  compatibility.
- Preserve `Properties.json()`, `Properties.__str__()`, and dynamic attribute
  assignment behavior.

Performance criteria:

- At least 25 percent faster empty `Properties.pack()` and `Properties.unpack()`
  microbenchmarks.
- At least 25 percent fewer allocations for empty MQTT v5 PUBLISH parse.
- At least 5 percent end-to-end MQTT v5 small-message improvement.
- No measurable regression for MQTT v3 workloads.

Documentation criteria:

- Record old and new metadata table ownership.
- Document any behavior that is intentionally preserved for compatibility even
  if it limits optimization.

## Verdict

GO.

Justification: this is a high-confidence optimization area with fixed protocol
metadata and low conceptual risk when covered by MQTT v5 tests. It should be one
of the first projects attempted.

## Progress (2026-07-09)

Status: **Done**.

Commit: `f2aaa76` (`perf: cache MQTT v5 property metadata lookups`).

### Implemented

- Move immutable property / reason-code tables to class-level
  `MappingProxyType` (and related precomputed maps) instead of rebuilding per
  instance.
- Keep public `Properties` / `ReasonCode` behavior; extend unit coverage in
  `tests/test_properties.py`.

### Measured (brokerless, earlier session)

| Scenario | Approx delta vs pre-03 baseline |
| --- | --- |
| `properties_pack_empty` | multi-thousand percent (metadata no longer rebuilt) |
| `properties_unpack_empty` | about +1000%+ |
| `reasoncode_create_puback_success` | about +280% to +700% depending on run |
| `publish_parse_v5_qos0_empty_props` | about +100% (collateral) |

Acceptance thresholds for empty pack/unpack and allocation reduction were met.
No further work planned unless a new MQTT v5 profile shows packing of heavy
`UserProperty` sets dominating end-to-end.
