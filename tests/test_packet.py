import pytest
from paho.mqtt import _packet
from paho.mqtt.enums import MQTTProtocolVersion
from paho.mqtt.exception import MalformedPacket
from paho.mqtt.properties import VariableByteIntegers


def split_full_packet(packet: bytes) -> tuple[int, bytes]:
    """ Split a full packet as header (int) and remaining_bytes

        It's used by most decode_xxx function. The full packet is
        `header + encoded_remaining_length + remaining_payload`

        It match _packet_read `self._in_packet["command"]` and `self._in_packet["packet"]`
    """

    header = packet[0]
    remaining_length, vbi_length = VariableByteIntegers.decode(packet[1:])
    if remaining_length + vbi_length + 1 != len(packet):
        raise ValueError("packet size mismatch")

    return header, packet[1+vbi_length:]


def test_decode_publish():
    result = _packet.decode_publish(
        *split_full_packet(b"\x30\x04\x00\x01\x61\x62"),
        MQTTProtocolVersion.MQTTv31,
    )
    assert result.qos == 0
    assert result.topic == b"a"
    assert result.payload == b"b"

    result = _packet.decode_publish(
        *split_full_packet(b"\x3b\x16\x00\x05topic\x01\x2a\x05\x02\x00\x00\x00\x2apayload"),
        MQTTProtocolVersion.MQTTv5,
    )
    assert result.qos == 1
    assert result.dup is True
    assert result.retain is True
    assert result.topic == b"topic"
    assert result.mid == 0x012a
    assert result.properties.MessageExpiryInterval == 42
    assert result.payload == b"payload"


def test_decode_publish_malformed():
    with pytest.raises(MalformedPacket):
        _packet.decode_publish(
            # Malformed. The topic length announce 64k but no bytes present
            *split_full_packet(b"\x30\x02\xFF\xFF"),
            MQTTProtocolVersion.MQTTv31,
        )


def test_decode_connack():
    result = _packet.decode_connack(
        *split_full_packet(b"\x20\x03\x00\x00\x00"),
        MQTTProtocolVersion.MQTTv5,
    )
    assert result.flags == 0
    assert result.reason.value == 0
    assert result.properties.isEmpty()


def test_decode_connack_malformed():
    with pytest.raises(MalformedPacket):
        _packet.decode_connack(
            # Malformed. The VBI ends with continuation bit
            *split_full_packet(b"\x20\x03\x00\x00\x80"),
            MQTTProtocolVersion.MQTTv5,
        )
