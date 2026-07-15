# 12 - Outbound Topic Encoding Cache

## Analysis

Repeated `publish()` calls encode and validate the same topic. A small per-client cache could remove that work, but every high-cardinality workload would pay an additional dictionary lookup and retain topic data.

## Preparation

A prototype cache was limited to 16 entries and 64 KiB, with no eviction after saturation. It was tested with 1, 2, 8, 16, 17, 1,000, and all-distinct topics using the exact public QoS 0 publish path.

## Expected Gain

Priority: P1. Target +5% for recurring topic sets with less than 2% regression for high-cardinality input.

## Acceptance Criteria

- At least +5% for recurring sets.
- Less than 2% regression for 1,000/all-distinct topics.
- Bounded retained memory and no caching of invalid topics.

## Before Measurement

The uncached path sustains roughly 72k-90k msg/s depending on cardinality and run order.

## Implementation

A bounded dictionary prototype was implemented and measured. It was then removed from production code after failing the high-cardinality guardrail. Only this result record is retained.

## After Measurements

| Topics | Prototype delta |
| ---: | ---: |
| 1 | +7.7% |
| 2 | +14.6% |
| 8 | +17.6% |
| 16 | +32.7% |
| 17 | +9.7% |
| 1,000 | **-14.4%** |
| all distinct | +3.4% (noisy) |

## Results Analysis

The cache helps stable, low-cardinality publishers but imposes unacceptable lookup cost for a realistic gateway with many device topics. Eviction would add more bookkeeping and was not justified after this result.

## Verdict

**NO GO.** Do not reintroduce an implicit topic cache without a new profile or an explicit opt-in API proposal.
