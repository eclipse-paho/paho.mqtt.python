import types

import paho.mqtt.client as client
import pytest
from paho.mqtt.matcher import MQTTMatcher


class Test_client_function:
    """
    Tests on topic_matches_sub function in the client module
    """

    @pytest.mark.parametrize("sub,topic", [
        ("foo/bar", "foo/bar"),
        ("foo/+", "foo/bar"),
        ("foo/+/baz", "foo/bar/baz"),
        ("foo/+/#", "foo/bar/baz"),
        ("A/B/+/#", "A/B/B/C"),
        ("#", "foo/bar/baz"),
        ("#", "/foo/bar"),
        ("/#", "/foo/bar"),
        ("$SYS/bar", "$SYS/bar"),
    ])
    def test_matching(self, sub, topic):
        assert client.topic_matches_sub(sub, topic)


    @pytest.mark.parametrize("sub,topic", [
        ("test/6/#", "test/3"),
        ("foo/bar", "foo"),
        ("foo/+", "foo/bar/baz"),
        ("foo/+/baz", "foo/bar/bar"),
        ("foo/+/#", "fo2/bar/baz"),
        ("/#", "foo/bar"),
        ("#", "$SYS/bar"),
        ("$BOB/bar", "$SYS/bar"),
    ])
    def test_not_matching(self, sub, topic):
        assert not client.topic_matches_sub(sub, topic)


class TestMQTTMatcher:
    def test_iter_match_order_exact_plus_hash(self):
        matcher = MQTTMatcher()
        matcher["devices/device-0001/telemetry"] = "exact"
        matcher["devices/+/telemetry"] = "plus"
        matcher["devices/#"] = "hash"

        assert list(matcher.iter_match("devices/device-0001/telemetry")) == [
            "exact", "plus", "hash"
        ]

    def test_iter_match_is_lazy_generator(self):
        matcher = MQTTMatcher()
        matcher["a/b"] = "exact"
        matcher["a/+"] = "plus"
        matcher["a/#"] = "hash"
        matcher["#"] = "root"

        it = matcher.iter_match("a/b")
        assert isinstance(it, types.GeneratorType)
        assert next(it) == "exact"
        assert next(it) == "plus"
        # Remaining matches must still be available without rematching.
        assert list(it) == ["hash", "root"]

    def test_multiple_matching_filters(self):
        matcher = MQTTMatcher()
        matcher["a/b"] = 1
        matcher["a/+"] = 2
        matcher["a/#"] = 3
        matcher["#"] = 4

        assert list(matcher.iter_match("a/b")) == [1, 2, 3, 4]

    @pytest.mark.parametrize("sub,topic,should_match", [
        ("$SYS/bar", "$SYS/bar", True),
        ("$SYS/+", "$SYS/bar", True),
        ("$SYS/#", "$SYS/bar", True),
        ("+", "$SYS/bar", False),
        ("#", "$SYS/bar", False),
        ("+/bar", "$SYS/bar", False),
        ("#", "SYS/bar", True),
        ("+", "SYS", True),
    ])
    def test_dollar_topic_semantics(self, sub, topic, should_match):
        matcher = MQTTMatcher()
        matcher[sub] = True
        assert bool(list(matcher.iter_match(topic))) is should_match

    def test_none_content_is_not_yielded(self):
        matcher = MQTTMatcher()
        matcher["foo/+"] = None
        matcher["foo/#"] = "hash"
        assert list(matcher.iter_match("foo/bar")) == ["hash"]
