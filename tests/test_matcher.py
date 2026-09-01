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
        # MQTT spec 4.7.1.2: '#' includes the parent level
        ("sport/#", "sport"),
        ("/#", "/"),
        ("sport/tennis/player1/#", "sport/tennis/player1"),
    ])
    def test_matching(self, sub, topic):
        assert client.topic_matches_sub(sub, topic)

    def test_hash_wildcard_matches_parent_level(self):
        """MQTT spec 4.7.1.2: sport/# matches the singular topic sport."""
        matcher = MQTTMatcher()
        matcher["sport/#"] = True
        assert list(matcher.iter_match("sport")) == [True]
        assert client.topic_matches_sub("sport/#", "sport")
        assert client.topic_matches_sub("/#", "/")


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
