# *******************************************************************
#   Copyright (c) 2017, 2019 IBM Corp.
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
#   Contributors:
#      Ian Craggs - initial implementation and/or documentation
# *******************************************************************

import struct
from types import MappingProxyType

from .packettypes import PacketTypes


_PACK_U16 = struct.Struct("!H")
_PACK_U32 = struct.Struct("!L")


class MQTTException(Exception):
    pass


class MalformedPacket(MQTTException):
    pass


def writeInt16(length):
    # serialize a 16 bit integer to network format
    return bytearray(_PACK_U16.pack(length))


def readInt16(buf):
    # deserialize a 16 bit integer from network format
    return _PACK_U16.unpack_from(buf, 0)[0]


def writeInt32(length):
    # serialize a 32 bit integer to network format
    return bytearray(_PACK_U32.pack(length))


def readInt32(buf):
    # deserialize a 32 bit integer from network format
    return _PACK_U32.unpack_from(buf, 0)[0]


def writeUTF(data):
    # data could be a string, or bytes.  If string, encode into bytes with utf-8
    if not isinstance(data, bytes):
        data = bytes(data, "utf-8")
    return writeInt16(len(data)) + data


def readUTF(buffer, maxlen):
    if maxlen >= 2:
        length = readInt16(buffer)
    else:
        raise MalformedPacket("Not enough data to read string length")
    maxlen -= 2
    if length > maxlen:
        raise MalformedPacket("Length delimited string too long")
    buf = buffer[2:2+length].decode("utf-8")
    # Python's strict UTF-8 decoder rejects encoded surrogate code points.
    # Native string searches avoid a Python/ord loop for valid MQTT strings.
    if "\x00" in buf:
        raise MalformedPacket("[MQTT-1.5.4-2] Null found in UTF-8 data")
    if "\ufeff" in buf:
        raise MalformedPacket("[MQTT-1.5.4-3] U+FEFF in UTF-8 data")
    return buf, length+2


def writeBytes(buffer):
    return writeInt16(len(buffer)) + buffer


def readBytes(buffer):
    length = readInt16(buffer)
    return buffer[2:2+length], length+2


class VariableByteIntegers:  # Variable Byte Integer
    """
    MQTT variable byte integer helper class.  Used
    in several places in MQTT v5.0 properties.

    """

    @staticmethod
    def encode(x):
        """
          Convert an integer 0 <= x <= 268435455 into multi-byte format.
          Returns the buffer converted from the integer.
        """
        if not 0 <= x <= 268435455:
            raise ValueError(f"Value {x!r} must be in range 0-268435455")
        buffer = b''
        while 1:
            digit = x % 128
            x //= 128
            if x > 0:
                digit |= 0x80
            buffer += bytes([digit])
            if x == 0:
                break
        return buffer

    @staticmethod
    def decode(buffer):
        """
          Get the value of a multi-byte integer from a buffer
          Return the value, and the number of bytes used.

          [MQTT-1.5.5-1] the encoded value MUST use the minimum number of bytes necessary to represent the value
        """
        return VariableByteIntegers.decode_at(buffer, 0, len(buffer))

    @staticmethod
    def decode_at(buffer, offset, end):
        multiplier = 1
        value = 0
        byte_count = 0
        while offset + byte_count < end:
            digit = buffer[offset + byte_count]
            byte_count += 1
            value += (digit & 127) * multiplier
            if digit & 128 == 0:
                return value, byte_count
            if byte_count == 4:
                raise MalformedPacket("Variable byte integer exceeds four bytes")
            multiplier *= 128
        raise MalformedPacket("Truncated variable byte integer")


class Properties:
    """MQTT v5.0 properties class.

    See Properties.names for a list of accepted property names along with their numeric values.

    See Properties.properties for the data type of each property.

    Example of use::

        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.UserProperty = ("a", "2")
        publish_properties.UserProperty = ("c", "3")

    First the object is created with packet type as argument, no properties will be present at
    this point. Then properties are added as attributes, the name of which is the string property
    name without the spaces.

    """

    types = ["Byte", "Two Byte Integer", "Four Byte Integer", "Variable Byte Integer",
             "Binary Data", "UTF-8 Encoded String", "UTF-8 String Pair"]

    names = MappingProxyType({
        "Payload Format Indicator": 1,
        "Message Expiry Interval": 2,
        "Content Type": 3,
        "Response Topic": 8,
        "Correlation Data": 9,
        "Subscription Identifier": 11,
        "Session Expiry Interval": 17,
        "Assigned Client Identifier": 18,
        "Server Keep Alive": 19,
        "Authentication Method": 21,
        "Authentication Data": 22,
        "Request Problem Information": 23,
        "Will Delay Interval": 24,
        "Request Response Information": 25,
        "Response Information": 26,
        "Server Reference": 28,
        "Reason String": 31,
        "Receive Maximum": 33,
        "Topic Alias Maximum": 34,
        "Topic Alias": 35,
        "Maximum QoS": 36,
        "Retain Available": 37,
        "User Property": 38,
        "Maximum Packet Size": 39,
        "Wildcard Subscription Available": 40,
        "Subscription Identifier Available": 41,
        "Shared Subscription Available": 42
    })

    _TYPE_BYTE = types.index("Byte")
    _TYPE_TWO_BYTE_INTEGER = types.index("Two Byte Integer")
    _TYPE_FOUR_BYTE_INTEGER = types.index("Four Byte Integer")
    _TYPE_VARIABLE_BYTE_INTEGER = types.index("Variable Byte Integer")
    _TYPE_BINARY_DATA = types.index("Binary Data")
    _TYPE_UTF8_STRING = types.index("UTF-8 Encoded String")
    _TYPE_UTF8_STRING_PAIR = types.index("UTF-8 String Pair")

    properties = MappingProxyType({
        # id:  type, packets
        1: (_TYPE_BYTE, (PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE)),
        2: (_TYPE_FOUR_BYTE_INTEGER, (PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE)),
        3: (_TYPE_UTF8_STRING, (PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE)),
        8: (_TYPE_UTF8_STRING, (PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE)),
        9: (_TYPE_BINARY_DATA, (PacketTypes.PUBLISH, PacketTypes.WILLMESSAGE)),
        11: (_TYPE_VARIABLE_BYTE_INTEGER, (PacketTypes.PUBLISH, PacketTypes.SUBSCRIBE)),
        17: (_TYPE_FOUR_BYTE_INTEGER, (PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.DISCONNECT)),
        18: (_TYPE_UTF8_STRING, (PacketTypes.CONNACK,)),
        19: (_TYPE_TWO_BYTE_INTEGER, (PacketTypes.CONNACK,)),
        21: (_TYPE_UTF8_STRING, (PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.AUTH)),
        22: (_TYPE_BINARY_DATA, (PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.AUTH)),
        23: (_TYPE_BYTE, (PacketTypes.CONNECT,)),
        24: (_TYPE_FOUR_BYTE_INTEGER, (PacketTypes.WILLMESSAGE,)),
        25: (_TYPE_BYTE, (PacketTypes.CONNECT,)),
        26: (_TYPE_UTF8_STRING, (PacketTypes.CONNACK,)),
        28: (_TYPE_UTF8_STRING, (PacketTypes.CONNACK, PacketTypes.DISCONNECT)),
        31: (_TYPE_UTF8_STRING, (
            PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
            PacketTypes.PUBREL, PacketTypes.PUBCOMP, PacketTypes.SUBACK,
            PacketTypes.UNSUBACK, PacketTypes.DISCONNECT, PacketTypes.AUTH)),
        33: (_TYPE_TWO_BYTE_INTEGER, (PacketTypes.CONNECT, PacketTypes.CONNACK)),
        34: (_TYPE_TWO_BYTE_INTEGER, (PacketTypes.CONNECT, PacketTypes.CONNACK)),
        35: (_TYPE_TWO_BYTE_INTEGER, (PacketTypes.PUBLISH,)),
        36: (_TYPE_BYTE, (PacketTypes.CONNACK,)),
        37: (_TYPE_BYTE, (PacketTypes.CONNACK,)),
        38: (_TYPE_UTF8_STRING_PAIR, (
            PacketTypes.CONNECT, PacketTypes.CONNACK,
            PacketTypes.PUBLISH, PacketTypes.PUBACK,
            PacketTypes.PUBREC, PacketTypes.PUBREL, PacketTypes.PUBCOMP,
            PacketTypes.SUBSCRIBE, PacketTypes.SUBACK,
            PacketTypes.UNSUBSCRIBE, PacketTypes.UNSUBACK,
            PacketTypes.DISCONNECT, PacketTypes.AUTH, PacketTypes.WILLMESSAGE)),
        39: (_TYPE_FOUR_BYTE_INTEGER, (PacketTypes.CONNECT, PacketTypes.CONNACK)),
        40: (_TYPE_BYTE, (PacketTypes.CONNACK,)),
        41: (_TYPE_BYTE, (PacketTypes.CONNACK,)),
        42: (_TYPE_BYTE, (PacketTypes.CONNACK,)),
    })

    _compressed_names_dict = {}
    _names_from_ident_dict = {}
    _compressed_names_from_ident_dict = {}
    for _name, _identifier in names.items():
        _compressed_name = _name.replace(' ', '')
        _compressed_names_dict[_compressed_name] = _identifier
        _names_from_ident_dict[_identifier] = _name
        _compressed_names_from_ident_dict[_identifier] = _compressed_name
    _compressed_names = MappingProxyType(_compressed_names_dict)
    _names_from_ident = MappingProxyType(_names_from_ident_dict)
    _compressed_names_from_ident = MappingProxyType(_compressed_names_from_ident_dict)
    _multiple_identifiers = frozenset((11, 38))
    _multiple_names_set = set()
    for _name, _identifier in _compressed_names.items():
        if _identifier in _multiple_identifiers:
            _multiple_names_set.add(_name)
    _multiple_names = frozenset(_multiple_names_set)
    _property_order_list = []
    for _name in names.keys():
        _property_order_list.append(_name.replace(' ', ''))
    _property_order = tuple(_property_order_list)
    _private_vars = frozenset(("packetType", "types", "names", "properties", "_set_properties"))
    del _compressed_names_dict, _names_from_ident_dict, _compressed_names_from_ident_dict
    del _multiple_names_set, _property_order_list, _name, _identifier, _compressed_name

    def __init__(self, packetType):
        object.__setattr__(self, "packetType", packetType)
        object.__setattr__(self, "_set_properties", set())

    def allowsMultiple(self, compressedName):
        if ' ' in compressedName:
            compressedName = compressedName.replace(' ', '')
        return compressedName in self._multiple_names

    def getIdentFromName(self, compressedName):
        # return the identifier corresponding to the property name
        if ' ' in compressedName:
            compressedName = compressedName.replace(' ', '')
        return self._compressed_names.get(compressedName, -1)

    def __setattr__(self, name, value):
        if ' ' in name:
            name = name.replace(' ', '')
        if name in self._private_vars:
            object.__setattr__(self, name, value)
        else:
            # the name could have spaces in, or not.  Remove spaces before assignment
            identifier = self._compressed_names.get(name)
            if identifier is None:
                raise MQTTException(
                    f"Property name must be one of {self.names.keys()}")
            # check that this attribute applies to the packet type
            if self.packetType not in self.properties[identifier][1]:
                raise MQTTException(f"Property {name} does not apply to packet type {PacketTypes.Names[self.packetType]}")

            # Check for forbidden values
            if not isinstance(value, list):
                if name in ["ReceiveMaximum", "TopicAlias"] \
                        and (value < 1 or value > 65535):

                    raise MQTTException(f"{name} property value must be in the range 1-65535")
                elif name in ["TopicAliasMaximum"] \
                        and (value < 0 or value > 65535):

                    raise MQTTException(f"{name} property value must be in the range 0-65535")
                elif name in ["MaximumPacketSize", "SubscriptionIdentifier"] \
                        and (value < 1 or value > 268435455):

                    raise MQTTException(f"{name} property value must be in the range 1-268435455")
                elif name in ["RequestResponseInformation", "RequestProblemInformation", "PayloadFormatIndicator"] \
                        and (value != 0 and value != 1):

                    raise MQTTException(
                        f"{name} property value must be 0 or 1")

            if name in self._multiple_names:
                if not isinstance(value, list):
                    value = [value]
                if hasattr(self, name):
                    value = object.__getattribute__(self, name) + value
            self._set_properties.add(name)
            object.__setattr__(self, name, value)

    def __delattr__(self, name):
        if ' ' in name:
            name = name.replace(' ', '')
        object.__delattr__(self, name)
        if name not in self._private_vars:
            self._set_properties.discard(name)

    def __str__(self):
        buffer = "["
        first = True
        for compressedName in self._property_order:
            if hasattr(self, compressedName):
                if not first:
                    buffer += ", "
                buffer += f"{compressedName} : {getattr(self, compressedName)}"
                first = False
        buffer += "]"
        return buffer

    def json(self):
        data = {}
        for compressedName in self._property_order:
            if hasattr(self, compressedName):
                val = getattr(self, compressedName)
                if compressedName == 'CorrelationData' and isinstance(val, bytes):
                    data[compressedName] = val.hex()
                else:
                    data[compressedName] = val
        return data

    def isEmpty(self):
        return len(self._set_properties) == 0

    def clear(self):
        for compressedName in tuple(self._set_properties):
            if hasattr(self, compressedName):
                delattr(self, compressedName)

    def writeProperty(self, identifier, type, value):
        buffer = bytearray()
        buffer.extend(VariableByteIntegers.encode(identifier))  # identifier
        if type == self._TYPE_BYTE:  # value
            buffer.append(value)
        elif type == self._TYPE_TWO_BYTE_INTEGER:
            buffer.extend(writeInt16(value))
        elif type == self._TYPE_FOUR_BYTE_INTEGER:
            buffer.extend(writeInt32(value))
        elif type == self._TYPE_VARIABLE_BYTE_INTEGER:
            buffer.extend(VariableByteIntegers.encode(value))
        elif type == self._TYPE_BINARY_DATA:
            buffer.extend(writeBytes(value))
        elif type == self._TYPE_UTF8_STRING:
            buffer.extend(writeUTF(value))
        elif type == self._TYPE_UTF8_STRING_PAIR:
            buffer.extend(writeUTF(value[0]))
            buffer.extend(writeUTF(value[1]))
        return buffer

    def pack(self):
        # serialize properties into buffer for sending over network
        if not self._set_properties:
            return b"\x00"

        buffer = bytearray()
        for compressedName in self._property_order:
            if compressedName in self._set_properties:
                identifier = self._compressed_names[compressedName]
                attr_type = self.properties[identifier][0]
                if compressedName in self._multiple_names:
                    for prop in getattr(self, compressedName):
                        buffer.extend(self.writeProperty(identifier, attr_type, prop))
                else:
                    buffer.extend(self.writeProperty(identifier, attr_type, getattr(self, compressedName)))
        return VariableByteIntegers.encode(len(buffer)) + buffer

    def readProperty(self, buffer, type, propslen):
        if type == self._TYPE_BYTE:
            value = buffer[0]
            valuelen = 1
        elif type == self._TYPE_TWO_BYTE_INTEGER:
            value = readInt16(buffer)
            valuelen = 2
        elif type == self._TYPE_FOUR_BYTE_INTEGER:
            value = readInt32(buffer)
            valuelen = 4
        elif type == self._TYPE_VARIABLE_BYTE_INTEGER:
            value, valuelen = VariableByteIntegers.decode(buffer)
        elif type == self._TYPE_BINARY_DATA:
            value, valuelen = readBytes(buffer)
        elif type == self._TYPE_UTF8_STRING:
            value, valuelen = readUTF(buffer, propslen)
        elif type == self._TYPE_UTF8_STRING_PAIR:
            value, valuelen = readUTF(buffer, propslen)
            buffer = buffer[valuelen:]  # strip the bytes used by the value
            value1, valuelen1 = readUTF(buffer, propslen - valuelen)
            value = (value, value1)
            valuelen += valuelen1
        return value, valuelen

    def getNameFromIdent(self, identifier):
        return self._names_from_ident.get(identifier)

    def unpack(self, buffer):
        self.clear()
        # deserialize properties into attributes from buffer received from network
        if buffer and buffer[0] == 0:
            return self, 1
        buffer_len = len(buffer)
        propslen, VBIlen = VariableByteIntegers.decode_at(buffer, 0, buffer_len)
        pos = VBIlen
        properties_end = pos + propslen
        if properties_end > buffer_len:
            raise MalformedPacket("Properties length exceeds packet length")

        while pos < properties_end:
            identifier, identifier_len = VariableByteIntegers.decode_at(buffer, pos, properties_end)
            pos += identifier_len
            attr_type = self.properties[identifier][0]
            value, pos = self._read_property_at(buffer, attr_type, pos, properties_end)
            compressedName = self._compressed_names_from_ident[identifier]
            if compressedName not in self._multiple_names and hasattr(self, compressedName):
                raise MQTTException(
                    f"Property '{compressedName}' must not exist more than once")
            setattr(self, compressedName, value)
        return self, propslen + VBIlen

    @staticmethod
    def _read_utf_at(buffer, pos, end):
        if pos + 2 > end:
            raise MalformedPacket("Not enough data to read string length")
        length = _PACK_U16.unpack_from(buffer, pos)[0]
        value_start = pos + 2
        value_end = value_start + length
        if value_end > end:
            raise MalformedPacket("Length delimited string too long")
        # str(..., "utf-8") decodes bytes, bytearray and memoryview alike;
        # unpack() may receive a memoryview slice of the read buffer.
        value = str(buffer[value_start:value_end], "utf-8")
        if "\x00" in value:
            raise MalformedPacket("[MQTT-1.5.4-2] Null found in UTF-8 data")
        if "\ufeff" in value:
            raise MalformedPacket("[MQTT-1.5.4-3] U+FEFF in UTF-8 data")
        return value, value_end

    def _read_property_at(self, buffer, property_type, pos, end):
        if property_type == self._TYPE_BYTE:
            if pos >= end:
                raise MalformedPacket("Not enough data to read byte property")
            return buffer[pos], pos + 1
        if property_type == self._TYPE_TWO_BYTE_INTEGER:
            if pos + 2 > end:
                raise MalformedPacket("Not enough data to read two-byte property")
            return _PACK_U16.unpack_from(buffer, pos)[0], pos + 2
        if property_type == self._TYPE_FOUR_BYTE_INTEGER:
            if pos + 4 > end:
                raise MalformedPacket("Not enough data to read four-byte property")
            return _PACK_U32.unpack_from(buffer, pos)[0], pos + 4
        if property_type == self._TYPE_VARIABLE_BYTE_INTEGER:
            value, value_len = VariableByteIntegers.decode_at(buffer, pos, end)
            return value, pos + value_len
        if property_type == self._TYPE_BINARY_DATA:
            if pos + 2 > end:
                raise MalformedPacket("Not enough data to read binary property")
            length = _PACK_U16.unpack_from(buffer, pos)[0]
            value_start = pos + 2
            value_end = value_start + length
            if value_end > end:
                raise MalformedPacket("Length delimited binary data too long")
            # Copy out: the underlying read buffer is reused between packets.
            return bytes(buffer[value_start:value_end]), value_end
        if property_type == self._TYPE_UTF8_STRING:
            return self._read_utf_at(buffer, pos, end)
        if property_type == self._TYPE_UTF8_STRING_PAIR:
            first, pos = self._read_utf_at(buffer, pos, end)
            second, pos = self._read_utf_at(buffer, pos, end)
            return (first, second), pos
        raise MQTTException(f"Unknown property type: {property_type}")
