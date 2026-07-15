# 11 - MQTT v5 Rich Property Codec

## Analysis

Rich MQTT v5 properties still used repeated buffer slicing, dynamic `struct.unpack()`, name normalization, and a Python `ord()` loop for every decoded UTF-8 character. Eight `UserProperty` pairs make these constant costs dominant.

## Preparation

The existing empty, common, eight-user-property, and end-to-end MQTT v5 PUBLISH scenarios are used. Malformed lengths, forbidden UTF-8 characters, duplicate properties, and encoded surrogates receive focused tests.

## Expected Gain

Priority: P1. Target +10% for rich unpack and +5% for rich PUBLISH parsing, with no regression in empty/common property operations.

## Acceptance Criteria

- Rich unpack improves by at least 10%.
- Rich PUBLISH parse improves by at least 5%.
- Empty/common property operations remain within 2% or improve.
- Public `Properties` APIs and validation behavior remain compatible.

## Before Measurement

Representative baseline medians on the unchanged audit commit:

| Scenario | Before |
| --- | ---: |
| Eight `UserProperty` unpack | 16,353 ops/s |
| Common property unpack | 65,696 ops/s |
| Empty property unpack | 434,810 ops/s |
| Rich MQTT v5 PUBLISH parse | 12,541 msg/s |

## Implementation

- Cached 16/32-bit `Struct` instances and used `unpack_from()`.
- Added cursor-based VBI and property decoding with explicit bounds checks.
- Precomputed identifier-to-compressed-name lookup.
- Avoided `replace()` for already canonical names.
- Replaced per-character UTF-8 validation with strict decode plus native NUL/BOM searches.
- Added a zero-property unpack fast path.

## After Measurements

Same-machine paired 15-run comparison:

| Scenario | After | Delta |
| --- | ---: | ---: |
| Eight `UserProperty` unpack | 25,091 ops/s | **+53.4%** |
| Common property unpack | 90,015 ops/s | **+37.0%** |
| Empty property unpack | 554,828 ops/s | **+27.6%** |
| Rich MQTT v5 PUBLISH parse | 16,066 msg/s | **+28.1%** |

## Results Analysis

Cursor parsing removes most temporary buffers, while UTF validation removes millions of Python-level `ord()` calls in rich sets. Empty properties remain fast through an explicit first-byte check.

## Verdict

**GO.** All property-codec thresholds are exceeded and malformed input is now bounded more explicitly.
