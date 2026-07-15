import struct
import threading

import paho.mqtt.client as client
import pytest
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties


class PartialRecvSocket:
    """Socket that exposes only the first `available` bytes; further recv raises BlockingIOError."""

    def __init__(self, data, available=0):
        self._data = bytes(data)
        self._pos = 0
        self.available = available
        self.calls = 0

    def recv(self, size):
        self.calls += 1
        if self._pos >= self.available:
            raise BlockingIOError()
        n = min(size, self.available - self._pos)
        chunk = self._data[self._pos : self._pos + n]
        self._pos += n
        return chunk

    def send(self, data):
        return len(data)

    def close(self):
        return None

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


class NonBlockingBurstSocket(PartialRecvSocket):
    """Expose a complete burst, then behave like an open non-blocking socket."""

    def __init__(self, data):
        super().__init__(data, available=len(data))

    def recv(self, size):
        if self._pos >= self.available:
            self.calls += 1
            raise BlockingIOError()
        return super().recv(size)


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


def _publish_packet(protocol, topic=b"t/a", payload=b"hi", qos=0, mid=1, properties=None):
    command = int(client.PUBLISH) | (qos << 1)
    variable = bytearray()
    variable.extend(struct.pack("!H", len(topic)))
    variable.extend(topic)
    if qos:
        variable.extend(struct.pack("!H", mid))
    if protocol == client.MQTTv5:
        if properties is None:
            variable.extend(b"\x00")
        else:
            variable.extend(properties.pack())
    remaining = len(variable) + len(payload)
    return bytes([command]) + _encode_varint(remaining) + bytes(variable) + payload


def _new_client(protocol=client.MQTTv311):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=protocol)
    mqttc.on_message = lambda mqttc, userdata, message: None
    return mqttc


def _new_ack_refill_client(total=8, ack_count=4):
    mqttc = _new_client()
    mqttc.on_publish = None
    mqttc._max_inflight_messages = ack_count
    mqttc._inflight_messages = ack_count
    mqttc._thread = threading.Thread(target=lambda: None)
    packets = bytearray()
    for mid in range(1, total + 1):
        message = client.MQTTMessage(mid=mid, topic=b"out/topic")
        message.payload = b"payload"
        message.qos = 1
        message.state = (
            client.mqtt_ms_wait_for_puback
            if mid <= ack_count
            else client.mqtt_ms_queued
        )
        mqttc._out_messages[mid] = message
        if mid <= ack_count:
            packets.extend(struct.pack("!BBH", int(client.PUBACK), 2, mid))
    mqttc._sock = NonBlockingBurstSocket(packets)
    return mqttc


def test_in_packet_state_is_reused_across_reads():
    mqttc = _new_client()
    packet = _publish_packet(client.MQTTv311)
    sock = PartialRecvSocket(packet * 2, available=len(packet) * 2)
    mqttc._sock = sock
    state = mqttc._in_packet

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc._in_packet is state
    assert state.command == 0
    assert state.remaining_count == 0
    assert len(state.packet) == 0

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc._in_packet is state


def test_internal_read_batch_prefetches_and_preserves_fairness_cap():
    mqttc = _new_client()
    packet = _publish_packet(client.MQTTv311)
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)
    sock = NonBlockingBurstSocket(packet * 250)
    mqttc._sock = sock

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 100
    assert mqttc._read_buffer_pending() > 0
    assert sock.calls == 1

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 200
    assert sock.calls == 1

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 250
    assert mqttc._read_buffer_pending() == 0
    assert sock.calls == 1


def test_internal_buffered_parser_dispatches_packet_view(monkeypatch):
    mqttc = _new_client()
    packet = _publish_packet(client.MQTTv311)
    sock = NonBlockingBurstSocket(packet)
    mqttc._sock = sock
    original_handle = mqttc._packet_handle
    packet_types = []

    def handle_packet():
        packet_types.append(type(mqttc._in_packet.packet))
        return original_handle()

    monkeypatch.setattr(mqttc, "_packet_handle", handle_packet)

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert packet_types == [memoryview]
    assert mqttc._in_packet.packet == bytearray()


def test_internal_buffered_parser_resumes_partial_packet():
    mqttc = _new_client()
    packet = _publish_packet(client.MQTTv311, payload=b"x" * 200)
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)
    sock = PartialRecvSocket(packet * 2, available=1)
    mqttc._sock = sock

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert messages == []
    assert mqttc._in_packet.command == packet[0]

    sock.available = len(packet) * 2
    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert [message.payload for message in messages] == [b"x" * 200, b"x" * 200]
    assert mqttc._in_packet.command == 0


def test_internal_buffered_parser_rejects_invalid_remaining_length():
    mqttc = _new_client()
    bad = bytes([int(client.PUBLISH), 0x80, 0x80, 0x80, 0x80, 0x01])
    mqttc._sock = NonBlockingBurstSocket(bad)

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_PROTOCOL


def test_internal_ack_batch_refills_inflight_once(monkeypatch):
    mqttc = _new_ack_refill_client()
    original_update = mqttc._update_inflight
    update_calls = []

    def update_inflight():
        update_calls.append(None)
        return original_update()

    monkeypatch.setattr(mqttc, "_update_inflight", update_inflight)

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(update_calls) == 1
    assert mqttc._inflight_messages == 4
    assert len(mqttc._out_messages) == 4
    assert len(mqttc._out_packet) == 4
    assert mqttc._inflight_refill_pending is False
    assert mqttc._inflight_refill_deferred is False


def test_public_packet_reads_refill_inflight_immediately(monkeypatch):
    mqttc = _new_ack_refill_client(total=4, ack_count=2)
    original_update = mqttc._update_inflight
    update_calls = []

    def update_inflight():
        update_calls.append(None)
        return original_update()

    monkeypatch.setattr(mqttc, "_update_inflight", update_inflight)

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc._inflight_messages == 2
    assert len(update_calls) == 1
    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc._inflight_messages == 2
    assert len(update_calls) == 2


def test_protocol_error_clears_deferred_inflight_refill():
    mqttc = _new_ack_refill_client()
    valid_ack = struct.pack("!BBH", int(client.PUBACK), 2, 1)
    invalid_packet = bytes([int(client.PUBLISH), 0x80, 0x80, 0x80, 0x80, 0x01])
    mqttc._sock = NonBlockingBurstSocket(valid_ack + invalid_packet)

    assert mqttc._loop_read_batch(100) == MQTTErrorCode.MQTT_ERR_PROTOCOL
    assert mqttc._inflight_refill_pending is False
    assert mqttc._inflight_refill_deferred is False


def test_public_packet_read_does_not_enable_read_ahead():
    mqttc = _new_client()
    packet = _publish_packet(client.MQTTv311)
    sock = NonBlockingBurstSocket(packet * 2)
    mqttc._sock = sock

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert sock.calls == 3
    assert mqttc._read_buffer_pending() == 0


def test_websocket_transport_bypasses_client_read_ahead():
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=client.MQTTv311,
        transport="websockets",
    )
    mqttc.on_message = lambda *args: None
    packet = _publish_packet(client.MQTTv311)
    sock = NonBlockingBurstSocket(packet * 2)
    mqttc._sock = sock

    assert mqttc._packet_read(read_ahead=True) == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert sock.calls == 3
    assert mqttc._read_buffer_pending() == 0


def test_sock_close_discards_prefetched_bytes():
    mqttc = _new_client()
    sock = NonBlockingBurstSocket(b"abcdef")
    mqttc._sock = sock

    assert mqttc._sock_recv_read_ahead(1) == b"a"
    assert mqttc._read_buffer_pending() == 5

    mqttc._sock_close()

    assert mqttc._read_buffer_pending() == 0


def test_puback_without_callback_skips_callback_metadata(monkeypatch):
    mqttc = _new_client()
    message = client.MQTTMessage(mid=7, topic=b"out/topic")
    message.qos = 1
    message.state = client.mqtt_ms_wait_for_puback
    mqttc._out_messages[7] = message
    mqttc._inflight_messages = 1
    mqttc._max_inflight_messages = 0
    mqttc._sock = PartialRecvSocket(b"\x40\x02\x00\x07", available=4)

    def unexpected_metadata(*args, **kwargs):
        raise AssertionError("MQTT v3 ACK metadata should be lazy without on_publish")

    monkeypatch.setattr(client, "ReasonCode", unexpected_metadata)
    monkeypatch.setattr(client, "Properties", unexpected_metadata)

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert message.info.is_published() is True
    assert mqttc._out_messages == {}
    assert mqttc._inflight_messages == 0


def test_unknown_puback_without_callback_is_ignored(monkeypatch):
    mqttc = _new_client()
    mqttc._sock = PartialRecvSocket(b"\x40\x02\x00\x63", available=4)

    def unexpected_metadata(*args, **kwargs):
        raise AssertionError("unknown MQTT v3 ACK must not build callback metadata")

    monkeypatch.setattr(client, "ReasonCode", unexpected_metadata)
    monkeypatch.setattr(client, "Properties", unexpected_metadata)

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS


def test_partial_fixed_header_read_returns_again():
    mqttc = _new_client()
    packet = _publish_packet(client.MQTTv311)
    sock = PartialRecvSocket(packet, available=1)
    mqttc._sock = sock

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_AGAIN
    assert mqttc._in_packet.command == packet[0]
    assert mqttc._in_packet.have_remaining == 0

    sock.available = len(packet)
    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert mqttc._in_packet.command == 0


def test_partial_remaining_length_and_payload_reads():
    mqttc = _new_client()
    packet = _publish_packet(client.MQTTv311, payload=b"x" * 200)
    assert packet[1] & 0x80  # multi-byte remaining length
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)
    sock = PartialRecvSocket(packet, available=2)
    mqttc._sock = sock

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_AGAIN
    assert mqttc._in_packet.command != 0
    assert mqttc._in_packet.have_remaining == 0
    assert mqttc._in_packet.remaining_count == 1

    sock.available = 8
    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_AGAIN
    assert mqttc._in_packet.have_remaining == 1
    assert mqttc._in_packet.to_process > 0

    sock.available = len(packet)
    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 1
    assert messages[0].payload == b"x" * 200


def test_invalid_remaining_length_over_four_bytes():
    mqttc = _new_client()
    bad = bytes([int(client.PUBLISH), 0x80, 0x80, 0x80, 0x80, 0x01])
    mqttc._sock = PartialRecvSocket(bad, available=len(bad))

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_PROTOCOL


def test_handle_publish_v5_empty_properties_fast_path():
    mqttc = _new_client(protocol=client.MQTTv5)
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)
    packet = _publish_packet(client.MQTTv5, topic=b"sensors/1", payload=b"{}", properties=None)
    mqttc._sock = PartialRecvSocket(packet, available=len(packet))

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert len(messages) == 1
    assert messages[0].payload == b"{}"
    assert messages[0].properties is not None
    assert messages[0].properties.isEmpty()


def test_handle_publish_v5_with_user_properties():
    mqttc = _new_client(protocol=client.MQTTv5)
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)

    props = Properties(PacketTypes.PUBLISH)
    props.UserProperty = ("k", "v")
    packet = _publish_packet(client.MQTTv5, payload=b"p", properties=props)
    mqttc._sock = PartialRecvSocket(packet, available=len(packet))

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert messages[0].properties.UserProperty == [("k", "v")]


def test_handle_publish_qos1_mid_and_invalid_utf8_topic():
    mqttc = _new_client()
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)
    packet = _publish_packet(client.MQTTv311, topic=b"\xff", payload=b"x", qos=1, mid=42)
    mqttc._sock = PartialRecvSocket(packet, available=len(packet))
    mqttc.manual_ack_set(True)

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert messages[0].mid == 42
    assert messages[0]._topic == b"\xff"
    with pytest.raises(UnicodeDecodeError):
        _ = messages[0].topic


def test_handle_publish_skips_topic_decode_when_logging_disabled():
    mqttc = _new_client()
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)
    packet = _publish_packet(client.MQTTv311, topic=b"sensors/1", payload=b"x")
    mqttc._sock = PartialRecvSocket(packet, available=len(packet))

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert messages[0]._topic_str is None
    assert messages[0].topic == "sensors/1"


def test_handle_publish_decodes_topic_when_on_log_set():
    mqttc = _new_client()
    logs = []
    mqttc.on_log = lambda mqttc, userdata, level, buf: logs.append(buf)
    mqttc.on_message = lambda *args: None
    packet = _publish_packet(client.MQTTv311, topic=b"sensors/1", payload=b"x")
    mqttc._sock = PartialRecvSocket(packet, available=len(packet))

    assert mqttc._packet_read() == MQTTErrorCode.MQTT_ERR_SUCCESS
    assert any("sensors/1" in entry for entry in logs)


def test_in_packet_reset_clears_fields():
    mqttc = _new_client()
    state = mqttc._in_packet
    state.command = 0x30
    state.have_remaining = 1
    state.remaining_count = 3
    state.remaining_mult = 128
    state.remaining_length = 10
    state.packet.extend(b"abc")
    state.to_process = 4
    state.pos = 1

    state.reset()

    assert state.command == 0
    assert state.have_remaining == 0
    assert state.remaining_count == 0
    assert state.remaining_mult == 1
    assert state.remaining_length == 0
    assert len(state.packet) == 0
    assert state.to_process == 0
    assert state.pos == 0
    assert mqttc._in_packet is state
