"""Unit tests for client benchmark helpers."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path

CLIENT_DIR = Path(__file__).resolve().parents[1]
_client_dir = str(CLIENT_DIR)
# Always pin client modules first: pytest prepends benchmarks/, which would
# otherwise shadow client harness/scenarios with the brokerless microbench files.
while _client_dir in sys.path:
    sys.path.remove(_client_dir)
sys.path.insert(0, _client_dir)

from loadgen import parse_emqtt_output, nominal_rate, interval_for_rate  # noqa: E402
from metrics import (  # noqa: E402
    abba_order,
    compare_verdict,
    integrity_counts,
    latency_summary,
    median,
    percentile,
    sanitize_number,
)
from scenarios import SCENARIO_BY_NAME, expand_scenario, list_scenarios, estimate_suite  # noqa: E402
from workloads import (  # noqa: E402
    build_payload,
    callback_match_loadgen_topic,
    callback_match_topics,
    decode_header,
    encode_header,
    overlapping_match_filters,
    payload_len_for_remaining_length,
    remaining_length_size,
    rl_boundary_payloads,
    single_topic,
)


class MetricsTests(unittest.TestCase):
    def test_sanitize(self):
        self.assertIsNone(sanitize_number(float("nan")))
        self.assertIsNone(sanitize_number(float("inf")))
        self.assertEqual(sanitize_number(1.5), 1.5)

    def test_percentile_and_median(self):
        values = [1, 2, 3, 4, 5]
        self.assertEqual(median(values), 3)
        self.assertEqual(percentile(values, 100), 5)
        self.assertIsNone(percentile([], 50))

    def test_latency_p99_gate(self):
        samples = list(range(100))
        summary = latency_summary(samples, min_for_p99=10_000)
        self.assertFalse(summary["p99_published"])
        self.assertIsNone(summary["p99_ms"])
        big = list(range(10_000))
        summary2 = latency_summary(big, min_for_p99=10_000)
        self.assertTrue(summary2["p99_published"])
        self.assertIsNotNone(summary2["p99_ms"])

    def test_abba_order(self):
        self.assertEqual(abba_order(1), ["A", "B", "B", "A"])
        self.assertEqual(len(abba_order(4)), 16)
        self.assertEqual(abba_order(4).count("A"), 8)
        self.assertEqual(abba_order(4).count("B"), 8)

    def test_compare_inconclusive_on_noise(self):
        baseline = [100.0] * 8
        candidate = [101.0] * 8
        verdict = compare_verdict(baseline, candidate, min_effect_pct=3.0)
        self.assertEqual(verdict["verdict"], "inconclusive")

    def test_integrity(self):
        expected = range(1, 6)
        received = [1, 2, 2, 4, 3, 5]
        counts = integrity_counts(expected, received)
        self.assertEqual(counts["unique"], 5)
        self.assertEqual(counts["duplicates"], 1)
        self.assertEqual(counts["missing"], 0)
        self.assertGreaterEqual(counts["out_of_order"], 1)


class WorkloadTests(unittest.TestCase):
    def test_payload_sizes(self):
        self.assertEqual(build_payload("empty0"), b"")
        self.assertEqual(len(build_payload("binary64")), 64)
        self.assertEqual(len(build_payload("telemetry256")), 256)
        self.assertEqual(len(build_payload("block128k")), 128 * 1024)
        self.assertEqual(len(build_payload("block256k")), 256 * 1024)
        self.assertEqual(len(build_payload("block512k")), 512 * 1024)
        self.assertIsInstance(build_payload("telemetry256_str"), str)

    def test_header_roundtrip(self):
        header = encode_header(b"abcd1234", 7, 99, 99, 123456789)
        decoded = decode_header(header + b"extra")
        self.assertEqual(decoded["publisher_id"], 7)
        self.assertEqual(decoded["sequence"], 99)
        self.assertEqual(decoded["send_ns"], 123456789)

    def test_remaining_length_boundaries(self):
        topic = single_topic("abcd1234")
        for target in (126, 127, 128, 16383, 16384):
            payload_len = payload_len_for_remaining_length(topic, 0, target)
            self.assertEqual(remaining_length_size(topic, 0, payload_len), target)
        sizes = rl_boundary_payloads(topic, qos=0)
        self.assertIn("rl_127", sizes)
        self.assertIn("rl_128", sizes)

    def test_unsupported_features_guard(self):
        from harness import unsupported_features  # noqa: PLC0415 - avoids docker deps at module import

        self.assertEqual(unsupported_features({"payload": "telemetry256", "qos_publish": 0}), [])
        self.assertIn("receive_maximum", unsupported_features({"receive_maximum": 10}))
        self.assertIn("retained_count", unsupported_features({"retained_count": 10_000}))
        self.assertIn("session_outage", unsupported_features({"outage_s": 2.0}))
        self.assertIn("queue_rejection_protocol", unsupported_features({"submit_count": 150}))
        self.assertIn("properties_profile:topic_alias", unsupported_features({"properties_profile": "topic_alias"}))
        self.assertIn("connect_mode:tcp_concurrent", unsupported_features({"connect_mode": "tcp_concurrent"}))
        self.assertIn("topic_topology:fleet4k_zipf", unsupported_features({"topic_topology": "fleet4k_zipf"}))
        # Supported values must not be flagged.
        self.assertEqual(unsupported_features({"properties_profile": "realistic", "connect_mode": "tcp_serial"}), [])

    def test_callback_match_topics_align_with_loadgen_template(self):
        run_id = "abcd1234"
        topics = callback_match_topics(run_id, 3)
        self.assertEqual(
            topics,
            [
                "bench/abcd1234/org/acme/cb/0/data",
                "bench/abcd1234/org/acme/cb/1/data",
                "bench/abcd1234/org/acme/cb/2/data",
            ],
        )
        self.assertEqual(callback_match_loadgen_topic(run_id), "bench/abcd1234/org/acme/cb/%i/data")
        overlap = overlapping_match_filters(run_id, 8)
        self.assertEqual(len(overlap), 8)
        self.assertEqual(len(set(overlap)), 8)

    def test_regression_audit_scenarios_are_individually_addressable(self):
        self.assertEqual(SCENARIO_BY_NAME["sub_exact_qos1_capacity"].qos_publish, 1)
        self.assertEqual(SCENARIO_BY_NAME["sub_exact_qos1_capacity"].qos_subscribe, 1)
        self.assertEqual(SCENARIO_BY_NAME["pub_qos1_sendmsg_capacity"].inflight, 100)
        self.assertEqual(SCENARIO_BY_NAME["pub_payload_16k"].payload, "record16k")
        self.assertEqual(SCENARIO_BY_NAME["pub_segment_block_64k"].payload, "block64k")
        self.assertEqual(SCENARIO_BY_NAME["pub_segment_block_128k"].payload, "block128k")
        self.assertEqual(SCENARIO_BY_NAME["pub_segment_block_256k"].payload, "block256k")
        self.assertEqual(SCENARIO_BY_NAME["pub_segment_block_512k"].payload, "block512k")
        self.assertEqual(SCENARIO_BY_NAME["pub_segment_blob_1m"].payload, "blob1m")
        self.assertEqual(SCENARIO_BY_NAME["rtt_capacity_qos1"].cadence, "capacity")

    def test_capacity_extraction_works_for_smoke_runs(self):
        from harness import _capacity_from_result, source_identity  # noqa: PLC0415

        result = {
            "results": [{
                "point": {"qos_publish": 1},
                "summary": {"median": None},
                "runs": [{"status": "valid", "primary_msgs_per_s": 1234.0}],
            }],
        }
        self.assertEqual(_capacity_from_result(result, qos=1), 1234.0)
        identity = source_identity(str(CLIENT_DIR.parent.parent))
        self.assertEqual(identity["version"], "2.1.1.dev0")
        self.assertTrue(identity["module"].endswith("src/paho/mqtt/__init__.py"))


class LoadgenParserTests(unittest.TestCase):
    def test_parse_fixture(self):
        fixture = (CLIENT_DIR / "fixtures" / "emqtt_bench_sample.txt").read_text(encoding="utf-8")
        parsed = parse_emqtt_output(fixture)
        self.assertGreaterEqual(parsed["samples"], 2)
        self.assertEqual(parsed["last_rate"], 99725)
        self.assertEqual(parsed["last_total"], 2102563)
        self.assertEqual(parsed["rates"][0], 39.92)

    def test_nominal_rate(self):
        self.assertEqual(nominal_rate(20, 100), 200.0)
        self.assertEqual(interval_for_rate(20, 20000), 1)


class ScenarioRegistryTests(unittest.TestCase):
    def test_core_and_full_present(self):
        core = list_scenarios("core")
        full = list_scenarios("full")
        self.assertGreaterEqual(len(core), 10)
        self.assertGreaterEqual(len(full), 10)
        self.assertIn("pub_qos_sweep_telemetry", SCENARIO_BY_NAME)
        self.assertIn("network_matrix", SCENARIO_BY_NAME)

    def test_smoke_shortens_durations(self):
        points = expand_scenario(SCENARIO_BY_NAME["pub_qos_sweep_telemetry"], "smoke")
        self.assertTrue(all(p["duration_s"] <= 3.0 for p in points))
        self.assertTrue(all(p["non_comparable"] for p in points))

    def test_estimate(self):
        est = estimate_suite("core", "smoke", 1)
        self.assertGreater(est["points"], 0)
        self.assertEqual(est["runs_per_point"], 1)


class SchemaTests(unittest.TestCase):
    def test_schema_file_exists_and_parses(self):
        schema_path = CLIENT_DIR / "result.schema.json"
        data = json.loads(schema_path.read_text(encoding="utf-8"))
        self.assertEqual(data["properties"]["schema_version"]["const"], 1)


if __name__ == "__main__":
    unittest.main()
