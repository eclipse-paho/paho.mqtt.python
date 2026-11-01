"""Regression tests for the Variable Byte Integer decoding bounds.

MQTT v5.0 section 1.5.5: "The maximum number of bytes in the Variable Byte
Integer field is four", and Table 1-1 caps the representable value at
268,435,455 (0xFF, 0xFF, 0xFF, 0x7F).  A fifth byte, or a buffer that ends
while a continuation bit is still pending, describes a packet that "cannot be
parsed according to this specification" -- a Malformed Packet (section 1.4),
which section 2.2.2.2 requires the receiver to report.

`VariableByteIntegers` frames the Property Length field, so an unbounded decode
lets a peer drive the property parser far past the end of its region.
"""

import struct

import pytest
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import MalformedPacket, Properties, VariableByteIntegers


class TestVariableByteIntegerFourByteCeiling:
    def test_fifth_byte_is_malformed(self):
        # Continuation bit still set after the fourth byte.
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(b"\xff\xff\xff\xff\x7f")

    def test_overlong_encoding_of_zero_is_malformed(self):
        # Five bytes encoding 0: past the ceiling regardless of the value.
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(b"\x80\x80\x80\x80\x00")

    def test_buffer_ending_with_pending_continuation_is_malformed(self):
        # Upstream walked off the end of the buffer and leaked IndexError.
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(b"\x80")

    def test_empty_buffer_is_malformed(self):
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(b"")

    def test_truncated_three_byte_value_is_malformed(self):
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(b"\x80\x80")


class TestVariableByteIntegerAcceptedRange:
    """The ceiling must not reject any well-formed encoding."""

    @pytest.mark.parametrize(
        "buffer, expected",
        [
            (b"\x00", 0),
            (b"\x7f", 127),
            (b"\x80\x01", 128),
            (b"\xff\x7f", 16383),
            (b"\x80\x80\x01", 16384),
            (b"\xff\xff\x7f", 2097151),
            (b"\x80\x80\x80\x01", 2097152),
            (b"\xff\xff\xff\x7f", 268435455),
        ],
    )
    def test_one_to_four_byte_values_still_decode(self, buffer, expected):
        value, used = VariableByteIntegers.decode(buffer)
        assert value == expected
        assert used == len(buffer)

    @pytest.mark.parametrize("value", [0, 1, 127, 128, 16383, 16384, 268435455])
    def test_encode_decode_round_trip(self, value):
        buffer = VariableByteIntegers.encode(value)
        assert len(buffer) <= 4
        assert VariableByteIntegers.decode(buffer) == (value, len(buffer))


class TestPropertyLengthUsesTheCeiling:
    def test_overlong_property_length_is_malformed(self):
        # A five-byte Property Length prefix would otherwise make the parser
        # read 34,359,738,367 bytes of "properties" straight out of the packet.
        correlation = bytes([9]) + struct.pack("!H", 2) + b"\xaa\xbb"
        bad = b"\xff\xff\xff\xff\x7f" + correlation
        packet = Properties(PacketTypes.PUBLISH)
        with pytest.raises(MalformedPacket):
            packet.unpack(bad)
