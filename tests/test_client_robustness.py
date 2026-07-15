"""Robustness regression tests added by the static performance-audit review.

These tests lock in behaviours that the optimisation work relies on but that
were not directly covered elsewhere: inbound QoS 2 inflight accounting,
truncated inbound PUBLISH handling, read-ahead chunk-boundary parsing,
Properties instance reuse, malformed variable byte integers, and socketpair
wakeup re-arming.
"""

import builtins
import struct
import threading
import urllib.request
from types import SimpleNamespace

import pytest

import paho.mqtt.client as client
from paho.mqtt.client import _READAHEAD_CHUNK_SIZE
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import MalformedPacket, Properties, VariableByteIntegers


def _fake_socks(default_proxy=None):
    return SimpleNamespace(
        HTTP=1,
        SOCKS4=2,
        SOCKS5=3,
        get_default_proxy=lambda: default_proxy,
    )


def test_get_proxy_without_pysocks_does_not_import_urllib(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    monkeypatch.setattr(client, "socks", None)
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.startswith("urllib"):
            raise AssertionError("urllib must stay lazy without PySocks")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert mqttc._get_proxy() is None


def test_get_proxy_explicit_configuration_stays_on_fast_path(monkeypatch):
    fake_socks = _fake_socks()
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    explicit = {
        "proxy_type": fake_socks.HTTP,
        "proxy_addr": "proxy.example",
        "proxy_port": 8080,
    }
    mqttc._proxy = explicit
    monkeypatch.setattr(client, "socks", fake_socks)
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.startswith("urllib"):
            raise AssertionError("explicit proxy must not discover environment proxies")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert mqttc._get_proxy() is explicit


def test_get_proxy_preserves_environment_and_no_proxy_behaviour(monkeypatch):
    fake_socks = _fake_socks((3, "default.example", 1080, True, "user", "pass"))
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._host = "broker.example"
    monkeypatch.setattr(client, "socks", fake_socks)
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
    monkeypatch.setattr(
        urllib.request,
        "getproxies",
        lambda: {"mqtt": "http://env-proxy.example:3128"},
    )

    assert mqttc._get_proxy() == {
        "proxy_type": fake_socks.HTTP,
        "proxy_addr": "env-proxy.example",
        "proxy_port": 3128,
    }

    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: True)
    assert mqttc._get_proxy() == {
        "proxy_type": fake_socks.SOCKS5,
        "proxy_addr": "default.example",
        "proxy_port": 1080,
        "proxy_rdns": True,
        "proxy_username": "user",
        "proxy_password": "pass",
    }


class BurstSocket:
    """Expose all bytes, then behave like an open non-blocking socket."""

    def __init__(self, data=b""):
        self._data = bytes(data)
        self._pos = 0
        self.recv_calls = 0
        self.sent = bytearray()

    def recv(self, size):
        self.recv_calls += 1
        if self._pos >= len(self._data):
            raise BlockingIOError()
        n = min(size, len(self._data) - self._pos)
        chunk = self._data[self._pos : self._pos + n]
        self._pos += n
        return chunk

    def send(self, data):
        self.sent.extend(bytes(data))
        return len(data)

    def close(self):
        return None

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


def _encode_varint(value):
    encoded = bytearray()
    while True:
        byte = value % 128
        value //= 128
        if value:
            byte |= 0x80
        encoded.append(byte)
        if not value:
            return bytes(encoded)


def _publish_packet(topic=b"t/a", payload=b"hi", qos=0, mid=1):
    command = int(client.PUBLISH) | (qos << 1)
    variable = bytearray()
    variable.extend(struct.pack("!H", len(topic)))
    variable.extend(topic)
    if qos:
        variable.extend(struct.pack("!H", mid))
    remaining = len(variable) + len(payload)
    return bytes([command]) + _encode_varint(remaining) + bytes(variable) + payload


def _pubrel_packet(mid):
    return struct.pack("!BBH", int(client.PUBREL) | 0x02, 2, mid)


def _new_client(protocol=client.MQTTv311):
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2, protocol=protocol
    )
    mqttc.on_message = lambda mqttc, userdata, message: None
    return mqttc


# ---------------------------------------------------------------------------
# Inbound QoS 2 flow must not corrupt the outgoing inflight counter.
# Historically _handle_pubrel decremented _inflight_messages even though
# inbound messages never increment it, silently widening the outgoing
# inflight window (see upstream PR #286).
# ---------------------------------------------------------------------------

def test_inbound_qos2_flow_does_not_corrupt_outgoing_inflight_counter():
    mqttc = _new_client()
    delivered = []
    mqttc.on_message = lambda mqttc, userdata, message: delivered.append(message)
    data = _publish_packet(topic=b"in/q2", payload=b"p", qos=2, mid=5)
    sock = BurstSocket(data)
    mqttc._sock = sock

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert 5 in mqttc._in_messages
    assert mqttc._inflight_messages == 0

    sock2 = BurstSocket(_pubrel_packet(5))
    sock2.sent = sock.sent
    mqttc._sock = sock2
    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS

    assert [message.payload for message in delivered] == [b"p"]
    assert mqttc._in_messages == {}
    # The outgoing inflight counter must remain untouched by the inbound flow.
    assert mqttc._inflight_messages == 0
    # PUBREC then PUBCOMP were sent.
    assert bytes(sock.sent[:4]) == struct.pack("!BBH", int(client.PUBREC), 2, 5)
    assert bytes(sock.sent[4:8]) == struct.pack("!BBH", int(client.PUBCOMP), 2, 5)


def test_duplicate_pubrel_is_acknowledged_without_second_delivery():
    mqttc = _new_client()
    delivered = []
    mqttc.on_message = lambda mqttc, userdata, message: delivered.append(message)
    sock = BurstSocket(
        _publish_packet(topic=b"in/q2", payload=b"p", qos=2, mid=9)
        + _pubrel_packet(9)
        + _pubrel_packet(9)
    )
    mqttc._sock = sock

    for _ in range(3):
        assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS

    assert len(delivered) == 1
    assert mqttc._inflight_messages == 0
    # PUBREC + two PUBCOMP (the duplicate PUBREL is still acknowledged).
    assert len(sock.sent) == 12


# ---------------------------------------------------------------------------
# Truncated inbound PUBLISH packets must yield a protocol error rather than
# raising (struct.error / IndexError) now that parsing is index-based.
# ---------------------------------------------------------------------------

def _raw_publish(command, body):
    return bytes([command]) + _encode_varint(len(body)) + body


@pytest.mark.parametrize(
    "packet",
    [
        # remaining length 1: too short to contain the topic length field
        _raw_publish(int(client.PUBLISH), b"\x00"),
        # declared topic length 10 with only 3 bytes available
        _raw_publish(int(client.PUBLISH), struct.pack("!H", 10) + b"abc"),
        # QoS 1 PUBLISH without space for the message id
        _raw_publish(int(client.PUBLISH) | 0x02, struct.pack("!H", 1) + b"t"),
    ],
)
def test_truncated_publish_returns_protocol_error(packet):
    mqttc = _new_client()
    mqttc._sock = BurstSocket(packet)

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_PROTOCOL


def test_truncated_publish_via_internal_batch_returns_protocol_error():
    mqttc = _new_client()
    packet = _raw_publish(int(client.PUBLISH), struct.pack("!H", 10) + b"abc")
    mqttc._sock = BurstSocket(packet)

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_PROTOCOL


# ---------------------------------------------------------------------------
# Read-ahead chunk boundaries: packets straddling the 64 KiB prefetch edge
# and payloads larger than one prefetch chunk must be reassembled intact.
# ---------------------------------------------------------------------------

def test_packets_straddling_read_ahead_chunk_boundary_are_reassembled():
    mqttc = _new_client()
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)

    payloads = [bytes([index % 251]) * 1500 for index in range(50)]
    stream = b"".join(
        _publish_packet(topic=b"t/%d" % index, payload=payload)
        for index, payload in enumerate(payloads)
    )
    assert len(stream) > _READAHEAD_CHUNK_SIZE
    mqttc._sock = BurstSocket(stream)

    for _ in range(10):  # more batches than ever needed; avoids infinite loop on failure
        assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
        if len(messages) == 50 and mqttc._read_buffer_pending() == 0:
            break

    assert [message.payload for message in messages] == payloads
    assert [message.topic for message in messages] == [
        "t/%d" % index for index in range(50)
    ]


def test_payload_larger_than_read_ahead_chunk_is_reassembled():
    mqttc = _new_client()
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)

    payload = bytes(range(256)) * 800  # 200 KiB > 64 KiB chunk
    mqttc._sock = BurstSocket(_publish_packet(topic=b"big", payload=payload))

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 1
    assert bytes(messages[0].payload) == payload


def test_public_packet_read_consumes_prefetched_bytes_before_socket():
    mqttc = _new_client()
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)
    packet = _publish_packet()
    sock = BurstSocket(packet * 3)
    mqttc._sock = sock

    # Internal batch limited to one packet leaves prefetched bytes behind.
    assert mqttc._loop_read_batch(1) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 1
    assert mqttc._read_buffer_pending() > 0
    calls_after_batch = sock.recv_calls

    # The public read path must drain the prefetched bytes first.
    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 3
    assert mqttc._read_buffer_pending() == 0
    assert sock.recv_calls == calls_after_batch


# ---------------------------------------------------------------------------
# Properties: instance reuse and malformed inputs.
# ---------------------------------------------------------------------------

def test_properties_unpack_clears_previous_state_on_reuse():
    props = Properties(PacketTypes.PUBLISH)
    props.ContentType = "application/json"
    assert not props.isEmpty()

    # Fast path: an empty property block must clear previous attributes.
    props.unpack(b"\x00")
    assert props.isEmpty()
    assert not hasattr(props, "ContentType")

    props.ContentType = "application/json"
    other = Properties(PacketTypes.PUBLISH)
    other.ResponseTopic = "resp/topic"
    packed = other.pack()

    props.unpack(packed)
    assert not hasattr(props, "ContentType")
    assert props.ResponseTopic == "resp/topic"


def test_properties_unpack_unknown_identifier_raises():
    props = Properties(PacketTypes.PUBLISH)
    # length 2, identifier 127 (unknown), one payload byte
    with pytest.raises((KeyError, MalformedPacket)):
        props.unpack(bytes([2, 0x7F, 0x00]))


def test_variable_byte_integer_rejects_over_long_encoding():
    with pytest.raises(MalformedPacket):
        VariableByteIntegers.decode(b"\x80\x80\x80\x80\x01")


def test_variable_byte_integer_rejects_truncated_encoding():
    with pytest.raises(MalformedPacket):
        VariableByteIntegers.decode(b"\x80")


def test_variable_byte_integer_roundtrip_boundaries():
    for value in (0, 127, 128, 16383, 16384, 2097151, 2097152, 268435455):
        encoded = VariableByteIntegers.encode(value)
        decoded, consumed = VariableByteIntegers.decode(encoded)
        assert decoded == value
        assert consumed == len(encoded)


# ---------------------------------------------------------------------------
# MQTTMessageInfo lazy-condition contract.
# ---------------------------------------------------------------------------

def test_wait_for_publish_returns_immediately_when_already_published():
    info = client.MQTTMessageInfo(3)
    info._set_as_published()

    info.wait_for_publish(timeout=5.0)
    # The early-return path must not allocate a Condition.
    assert info._condition is None


def test_wait_for_publish_raises_for_failed_publish():
    info = client.MQTTMessageInfo(3)
    info.rc = MQTTErrorCode.MQTT_ERR_NO_CONN
    info._set_as_published()

    with pytest.raises(RuntimeError):
        info.wait_for_publish(timeout=0.1)


# ---------------------------------------------------------------------------
# MQTT v3 PUBACK with a user callback still delivers success metadata.
# ---------------------------------------------------------------------------

def test_v3_puback_with_callback_receives_success_metadata():
    mqttc = _new_client()
    results = []

    def on_publish(mqttc, userdata, mid, reason_code, properties):
        results.append((mid, reason_code, properties))

    mqttc.on_publish = on_publish
    message = client.MQTTMessage(mid=7, topic=b"out/topic")
    message.qos = 1
    message.state = client.mqtt_ms_wait_for_puback
    mqttc._out_messages[7] = message
    mqttc._inflight_messages = 1
    mqttc._sock = BurstSocket(struct.pack("!BBH", int(client.PUBACK), 2, 7))

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(results) == 1
    mid, reason_code, properties = results[0]
    assert mid == 7
    assert reason_code.value == 0
    assert properties.isEmpty()
    assert message.info.is_published() is True
    assert mqttc._inflight_messages == 0


# ---------------------------------------------------------------------------
# Socketpair wakeup coalescing must re-arm after the loop drains it.
# ---------------------------------------------------------------------------

def test_wake_thread_rearms_after_drain():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    sock = BurstSocket()
    sends = []
    sock.send = lambda data: (sends.append(bytes(data)), len(data))[1]
    mqttc._sockpairW = sock

    mqttc._wake_thread()
    mqttc._wake_thread()
    assert len(sends) == 1  # coalesced while pending

    # Simulate the network loop draining the wakeup byte.
    with mqttc._sockpair_wakeup_mutex:
        mqttc._sockpair_wakeup_pending = False

    mqttc._wake_thread()
    assert len(sends) == 2  # re-armed after drain


def test_wake_thread_without_sockpair_is_noop():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sockpairW = None
    mqttc._wake_thread()  # must not raise


# ---------------------------------------------------------------------------
# Threaded loop deadline selection edge cases.
# ---------------------------------------------------------------------------

def test_thread_loop_timeout_with_keepalive_disabled(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._keepalive = 0
    mqttc._last_msg_in = 100.0
    mqttc._last_msg_out = 100.0

    # Recent activity keeps the historical one-second tick.
    monkeypatch.setattr(client, "time_func", lambda: 100.5)
    assert mqttc._thread_loop_timeout(3600.0) == 1.0

    # Idle with keepalive disabled: wait for the full deadline.
    monkeypatch.setattr(client, "time_func", lambda: 200.0)
    assert mqttc._thread_loop_timeout(3600.0) == 3600.0


def test_thread_loop_timeout_never_negative(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._keepalive = 60
    mqttc._last_msg_in = 100.0
    mqttc._last_msg_out = 100.0

    # Far beyond the keepalive deadline the timeout must clamp at zero.
    monkeypatch.setattr(client, "time_func", lambda: 500.0)
    assert mqttc._thread_loop_timeout(60.0) == 0.0


# ---------------------------------------------------------------------------
# Segmented payload threshold boundary.
# ---------------------------------------------------------------------------

def test_segmentation_threshold_boundary():
    threshold = 1024 * 1024

    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = BurstSocket()
    mqttc._thread = threading.Thread(target=lambda: None)
    mqttc.publish("t", b"x" * (threshold - 1), qos=0)
    assert isinstance(mqttc._out_packet[-1]["packet"], bytearray)

    mqttc.publish("t", b"x" * threshold, qos=0)
    assert isinstance(mqttc._out_packet[-1]["packet"], tuple)
