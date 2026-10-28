"""Regression tests for Binary Data property decoding bounds.

A Binary Data property (MQTT v5.0 section 1.5.6) is a Two Byte Integer length
followed by that many octets, and every property is framed by the Property
Length field of section 2.2.2.1.  A declared length that reaches past the end
of the properties region therefore describes a packet that "cannot be parsed
according to this specification" -- a Malformed Packet (section 1.4), which
section 2.2.2.2 requires the receiver to report.

The sibling UTF-8 string reader (readUTF) already performs exactly this bound
check; the binary reader must do the same.
"""

import struct

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import MalformedPacket, Properties, VariableByteIntegers


def properties_field(props: bytes) -> bytes:
    """Build an on-the-wire properties region: Property Length VBI + props."""
    return VariableByteIntegers.encode(len(props)) + props


class TestBinaryDataBounds:
    def test_binary_length_past_property_length_is_malformed(self):
        # Correlation Data (identifier 9) declares 5 octets but only 2 are
        # present inside the declared Property Length.
        props = bytes([9]) + struct.pack("!H", 5) + b"\xaa\xbb"
        packet = Properties(PacketTypes.PUBLISH)
        with pytest.raises(MalformedPacket):
            packet.unpack(properties_field(props))

    def test_short_binary_buffer_is_not_silently_truncated(self):
        # Upstream returned the truncated value with no error at all; the
        # caller cannot tell a truncated property from a complete one.
        props = bytes([9]) + struct.pack("!H", 65535) + b"\xaa"
        packet = Properties(PacketTypes.PUBLISH)
        with pytest.raises(MalformedPacket):
            packet.unpack(properties_field(props))

    def test_off_by_one_binary_overrun_is_malformed(self):
        # The boundary case: the value ends one byte past the declared
        # Property Length, so it cannot be parsed.
        props = bytes([9]) + struct.pack("!H", 4) + b"\xaa\xbb\xcc"
        packet = Properties(PacketTypes.PUBLISH)
        with pytest.raises(MalformedPacket):
            packet.unpack(properties_field(props))

    def test_zero_length_binary_data_is_accepted(self):
        # A zero-octet Binary Data value overruns nothing, so it stays valid;
        # keep the accepted case green so the new bound does not over-tighten.
        props = bytes([9]) + struct.pack("!H", 0)
        packet = Properties(PacketTypes.PUBLISH)
        packet.unpack(properties_field(props))
        assert packet.CorrelationData == b""

    def test_exact_fit_binary_data_still_accepted(self):
        # The bound is "declared length > remaining bytes", so a value that
        # ends exactly at the end of the region must still decode.
        props = bytes([9]) + struct.pack("!H", 3) + b"\xaa\xbb\xcc"
        packet = Properties(PacketTypes.PUBLISH)
        packet.unpack(properties_field(props))
        assert packet.CorrelationData == b"\xaa\xbb\xcc"

    def test_binary_data_followed_by_another_property_still_accepted(self):
        correlation = bytes([9]) + struct.pack("!H", 2) + b"\xaa\xbb"
        response_topic = bytes([8]) + struct.pack("!H", 4) + b"chan"
        props = correlation + response_topic
        packet = Properties(PacketTypes.PUBLISH)
        packet.unpack(properties_field(props))
        assert packet.CorrelationData == b"\xaa\xbb"
        assert packet.ResponseTopic == "chan"
