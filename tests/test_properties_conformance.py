# *******************************************************************
#   Copyright (c) 2026 contributing authors
#
#   All rights reserved. This program and the accompanying materials
#   are made available under the terms of the Eclipse Public License v2.0
#   and Eclipse Distribution License v1.0 which accompany this distribution.
#
#   The Eclipse Public License is available at
#      http://www.eclipse.org/legal/epl-v20.html
#   and the Eclipse Distribution License is available at
#     http://www.eclipse.org/org/documents/edl-v10.php.
#
# *******************************************************************

"""
MQTT v5.0 property decoding conformance tests.

Every malformed property region must raise MalformedPacket - the client maps
that exception to a clean protocol-error disconnect - instead of letting
KeyError, struct.error or IndexError escape and kill the receive loop.
"""

import pytest

from paho.mqtt.exception import MalformedPacket
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties, VariableByteIntegers


def unpack_publish(payload):
    props, consumed = Properties(PacketTypes.PUBLISH).unpack(payload)
    return props, consumed


class TestUnknownIdentifier:
    def test_unknown_identifier_raises_malformed_packet(self):
        # propslen=1, identifier 0x7F is not a defined property
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x01, 0x7F]))

    def test_unknown_multibyte_identifier_raises_malformed_packet(self):
        # propslen=2, identifier 0x81 0x02 (257) is not defined
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x02, 0x81, 0x02]))


class TestFixedSizeProperties:
    def test_byte_property_with_empty_region_raises(self):
        # propslen=1 declares a Byte property but carries only the identifier
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x01, 0x01]))

    def test_four_byte_integer_with_short_region_raises(self):
        # propslen=2 declares a four byte integer property (identifier + 1 value byte)
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x02, 0x02, 0x01]))

    def test_two_byte_integer_with_short_region_raises(self):
        # propslen=2, identifier 19 (Server Keep Alive, two byte integer), 1 value byte
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x02, 0x13, 0x01]))

    def test_four_byte_integer_beyond_buffer_raises(self):
        # propslen=4 but the buffer carries only the identifier and two bytes
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x04, 0x02, 0x01, 0x02]))


class TestVariableByteIntegerProperties:
    def test_value_exceeding_property_length_raises(self):
        # propslen=2, identifier 11 (subscription identifier, VBI) encoded as
        # 0x80 0x01 (two bytes): the value reaches past the declared region
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x02, 0x0B, 0x80, 0x01]))

    def test_identifier_exceeding_property_length_raises(self):
        # propslen=1, identifier encoded with two bytes reaches past the region
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x01, 0x81, 0x02]))

    def test_value_consuming_payload_bytes_does_not_desync(self):
        # propslen=2, subscription identifier VBI 0x80 0x01 would consume the
        # two payload bytes 0xAA 0xBB; the parse must stop at the region.
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x02, 0x0B, 0x80, 0x01, 0xAA, 0xBB]))

    def test_valid_subscription_identifier_still_works(self):
        props, consumed = unpack_publish(bytes([0x02, 0x0B, 0x05, 0xAA]))
        assert props.SubscriptionIdentifier == [5]
        # only the two declared property bytes belong to the properties region
        assert consumed == 3


class TestVariableByteIntegerEncoding:
    def test_four_byte_maximum_accepted(self):
        value, used = VariableByteIntegers.decode(bytes([0xFF, 0xFF, 0xFF, 0x7F]))
        assert value == 268435455
        assert used == 4

    def test_five_byte_encoding_rejected(self):
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(bytes([0x80, 0x80, 0x80, 0x80, 0x01]))

    def test_empty_buffer_rejected(self):
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(b"")

    def test_non_minimal_encoding_rejected(self):
        # [MQTT-1.5.5-1] 129 must be encoded as 0x81 0x01 (two bytes), not
        # with a leading zero continuation digit: 0x81 0x81 0x80
        with pytest.raises(MalformedPacket):
            VariableByteIntegers.decode(bytes([0x81, 0x81, 0x80]))

    def test_minimal_encoding_accepted(self):
        value, used = VariableByteIntegers.decode(bytes([0x81, 0x01]))
        assert value == 129
        assert used == 2

    def test_non_minimal_property_value_rejected(self):
        # propslen=3, subscription identifier 5 encoded non-minimally
        with pytest.raises(MalformedPacket):
            unpack_publish(bytes([0x03, 0x0B, 0x85, 0x80, 0x00]))


class TestValidPropertiesUnchanged:
    def test_message_expiry_interval(self):
        props, _ = unpack_publish(bytes([0x05, 0x02, 0x00, 0x00, 0x0E, 0x10]))
        assert props.MessageExpiryInterval == 3600

    def test_payload_format_indicator(self):
        props, _ = unpack_publish(bytes([0x02, 0x01, 0x01]))
        assert props.PayloadFormatIndicator == 1

    def test_correlation_data(self):
        props, _ = unpack_publish(bytes([0x06, 0x09, 0x00, 0x03, 0x01, 0x02, 0x03]))
        assert props.CorrelationData == bytes([0x01, 0x02, 0x03])

    def test_content_type(self):
        props, _ = unpack_publish(bytes([0x06, 0x03, 0x00, 0x03]) + b"abc")
        assert props.ContentType == "abc"

    def test_empty_properties(self):
        props, consumed = unpack_publish(bytes([0x00]))
        assert props.isEmpty()
        assert consumed == 1
