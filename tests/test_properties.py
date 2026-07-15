import pytest

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import MalformedPacket, MQTTException, Properties, readUTF, writeUTF
from paho.mqtt.reasoncodes import ReasonCode


def test_empty_properties_pack_unpack_tracking():
    properties = Properties(PacketTypes.PUBLISH)

    assert properties.isEmpty()
    assert properties.pack() == b"\x00"

    unpacked, used = properties.unpack(b"\x00")

    assert unpacked is properties
    assert used == 1
    assert properties.isEmpty()


def test_unpack_accepts_memoryview_and_detaches_binary_properties():
    source = Properties(PacketTypes.PUBLISH)
    source.CorrelationData = b"binary-data"
    source.UserProperty = ("key", "value")
    packet = bytearray(source.pack())

    decoded = Properties(PacketTypes.PUBLISH)
    decoded.unpack(memoryview(packet))

    assert decoded.CorrelationData == b"binary-data"
    assert type(decoded.CorrelationData) is bytes
    assert decoded.UserProperty == [("key", "value")]

    packet[:] = b"\x00" * len(packet)
    assert decoded.CorrelationData == b"binary-data"


def test_user_property_allows_multiple_and_clear_updates_tracking():
    properties = Properties(PacketTypes.PUBLISH)

    properties.UserProperty = ("a", "1")
    properties.UserProperty = ("b", "2")

    assert not properties.isEmpty()
    assert properties.UserProperty == [("a", "1"), ("b", "2")]

    packed = properties.pack()
    unpacked = Properties(PacketTypes.PUBLISH)
    unpacked.unpack(packed)

    assert unpacked.UserProperty == [("a", "1"), ("b", "2")]

    unpacked.clear()
    assert unpacked.isEmpty()
    assert unpacked.pack() == b"\x00"


def test_deleting_property_updates_empty_tracking():
    properties = Properties(PacketTypes.PUBLISH)
    properties.PayloadFormatIndicator = 1

    assert not properties.isEmpty()

    del properties.PayloadFormatIndicator

    assert properties.isEmpty()
    assert properties.pack() == b"\x00"


def test_duplicate_single_property_unpack_still_fails():
    content_type = bytes([3]) + writeUTF("application/json")
    duplicate_packet = bytes([len(content_type) * 2]) + content_type + content_type

    with pytest.raises(MQTTException):
        Properties(PacketTypes.PUBLISH).unpack(duplicate_packet)


def test_property_name_lookup_uses_compressed_names():
    properties = Properties(PacketTypes.PUBLISH)

    assert properties.getIdentFromName("PayloadFormatIndicator") == 1
    assert properties.getIdentFromName("Payload Format Indicator") == 1
    assert properties.getNameFromIdent(1) == "Payload Format Indicator"
    assert properties.getIdentFromName("NotAProperty") == -1


def test_reason_code_invalid_for_packet_still_fails():
    with pytest.raises(ValueError):
        ReasonCode(PacketTypes.PUBACK, identifier=1)


@pytest.mark.parametrize("encoded", [b"\x00", "\ufeff".encode("utf-8")])
def test_read_utf_rejects_mqtt_forbidden_characters(encoded):
    field = len(encoded).to_bytes(2, "big") + encoded

    with pytest.raises(MalformedPacket):
        readUTF(field, len(field))


def test_read_utf_strict_decoder_rejects_encoded_surrogate():
    encoded_surrogate = b"\xed\xa0\x80"
    field = len(encoded_surrogate).to_bytes(2, "big") + encoded_surrogate

    with pytest.raises(UnicodeDecodeError):
        readUTF(field, len(field))


def test_property_unpack_rejects_truncated_utf_field():
    with pytest.raises(MalformedPacket):
        Properties(PacketTypes.PUBLISH).unpack(b"\x02\x03\x00")


def test_property_unpack_rejects_declared_length_beyond_packet():
    with pytest.raises(MalformedPacket):
        Properties(PacketTypes.PUBLISH).unpack(b"\x05\x03\x00\x01x")
