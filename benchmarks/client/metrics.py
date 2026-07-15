"""Statistical helpers and metric aggregation for client benchmarks."""

from __future__ import annotations

import math
import random
from typing import Iterable, List, Optional, Sequence


def sanitize_number(value: Optional[float]) -> Optional[float]:
    """Replace NaN/Inf with None for JSON-safe output."""
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    raise TypeError(f"unsupported numeric type: {type(value)!r}")


def percentile(values: Sequence[float], pct: float) -> Optional[float]:
    """Nearest-rank percentile; returns None for empty input."""
    if not values:
        return None
    if pct <= 0:
        return float(min(values))
    if pct >= 100:
        return float(max(values))
    ordered = sorted(values)
    rank = int(math.ceil((pct / 100.0) * len(ordered))) - 1
    rank = max(0, min(rank, len(ordered) - 1))
    return float(ordered[rank])


def median(values: Sequence[float]) -> Optional[float]:
    return percentile(values, 50.0)


def mad(values: Sequence[float]) -> Optional[float]:
    """Median absolute deviation."""
    med = median(values)
    if med is None:
        return None
    return median([abs(v - med) for v in values])


def mean(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    return float(sum(values) / len(values))


def summarize_runs(values: Sequence[float]) -> dict:
    cleaned = [float(v) for v in values if v is not None and not math.isnan(v) and not math.isinf(v)]
    return {
        "n": len(cleaned),
        "values": cleaned,
        "median": sanitize_number(median(cleaned)),
        "mad": sanitize_number(mad(cleaned)),
        "min": sanitize_number(min(cleaned) if cleaned else None),
        "max": sanitize_number(max(cleaned) if cleaned else None),
        "mean": sanitize_number(mean(cleaned)),
    }


def latency_summary(samples_ns: Sequence[int], *, min_for_p99: int = 10_000) -> dict:
    """Summarize latency samples in milliseconds with gated p99."""
    samples_ms = [s / 1_000_000.0 for s in samples_ns]
    result = {
        "n_success": len(samples_ms),
        "p50_ms": sanitize_number(percentile(samples_ms, 50.0)),
        "p95_ms": sanitize_number(percentile(samples_ms, 95.0)),
        "p99_ms": None,
        "max_ms": sanitize_number(max(samples_ms) if samples_ms else None),
        "p99_published": False,
    }
    if len(samples_ms) >= min_for_p99:
        result["p99_ms"] = sanitize_number(percentile(samples_ms, 99.0))
        result["p99_published"] = True
    return result


def bootstrap_median_diff(
    baseline: Sequence[float],
    candidate: Sequence[float],
    *,
    n_boot: int = 2000,
    seed: int = 42,
    confidence: float = 0.95,
) -> dict:
    """Paired-style bootstrap on unpaired samples via resampled median ratio."""
    if not baseline or not candidate:
        return {
            "median_ratio": None,
            "ci_low": None,
            "ci_high": None,
            "excludes_zero_effect": False,
            "absolute_effect_pct": None,
        }
    rng = random.Random(seed)
    base_med = median(baseline)
    cand_med = median(candidate)
    if base_med is None or cand_med is None or base_med == 0:
        return {
            "median_ratio": None,
            "ci_low": None,
            "ci_high": None,
            "excludes_zero_effect": False,
            "absolute_effect_pct": None,
        }
    ratio = cand_med / base_med
    diffs = []
    for _ in range(n_boot):
        b = [baseline[rng.randrange(len(baseline))] for _ in range(len(baseline))]
        c = [candidate[rng.randrange(len(candidate))] for _ in range(len(candidate))]
        bm = median(b)
        cm = median(c)
        if bm is None or cm is None or bm == 0:
            continue
        diffs.append((cm / bm) - 1.0)
    if not diffs:
        return {
            "median_ratio": sanitize_number(ratio),
            "ci_low": None,
            "ci_high": None,
            "excludes_zero_effect": False,
            "absolute_effect_pct": sanitize_number((ratio - 1.0) * 100.0),
        }
    alpha = 1.0 - confidence
    lo = percentile(diffs, 100.0 * (alpha / 2.0))
    hi = percentile(diffs, 100.0 * (1.0 - alpha / 2.0))
    excludes_zero = lo is not None and hi is not None and (lo > 0 or hi < 0)
    return {
        "median_ratio": sanitize_number(ratio),
        "ci_low": sanitize_number(lo),
        "ci_high": sanitize_number(hi),
        "excludes_zero_effect": excludes_zero,
        "absolute_effect_pct": sanitize_number((ratio - 1.0) * 100.0),
    }


def abba_order(blocks: int) -> List[str]:
    """Return ABBA repeated `blocks` times (4 blocks => 16 slots? No: 4 blocks of ABBA = 16).

    Plan: four ABBA blocks give 8 runs per source.
    Each ABBA block is A,B,B,A => 2A + 2B per block.
    4 blocks => 8A + 8B.
    """
    if blocks < 1:
        raise ValueError("blocks must be >= 1")
    order: List[str] = []
    for _ in range(blocks):
        order.extend(["A", "B", "B", "A"])
    return order


def compare_verdict(
    baseline_rates: Sequence[float],
    candidate_rates: Sequence[float],
    *,
    min_effect_pct: float = 3.0,
    seed: int = 42,
) -> dict:
    """Improvement/regression only if CI excludes 0 and |effect| > threshold."""
    boot = bootstrap_median_diff(baseline_rates, candidate_rates, seed=seed)
    effect = boot.get("absolute_effect_pct")
    excludes = boot.get("excludes_zero_effect", False)
    if effect is None or not excludes or abs(effect) <= min_effect_pct:
        verdict = "inconclusive"
    elif effect > 0:
        # Higher rate is better for throughput.
        verdict = "improvement"
    else:
        verdict = "regression"
    return {"verdict": verdict, **boot}


def integrity_counts(expected_sequences: Iterable[int], received_sequences: Iterable[int]) -> dict:
    """Compute unique/missing/duplicate/out-of-order counts for integrity runs."""
    expected = list(expected_sequences)
    received = list(received_sequences)
    expected_set = set(expected)
    seen = set()
    duplicates = 0
    out_of_order = 0
    last = -1
    for seq in received:
        if seq in seen:
            duplicates += 1
        else:
            seen.add(seq)
        if seq < last:
            out_of_order += 1
        last = seq
    unique = len(seen & expected_set)
    missing = len(expected_set - seen)
    unexpected = len(seen - expected_set)
    return {
        "expected": len(expected_set),
        "received": len(received),
        "unique": unique,
        "missing": missing,
        "duplicates": duplicates,
        "out_of_order": out_of_order,
        "unexpected": unexpected,
    }
