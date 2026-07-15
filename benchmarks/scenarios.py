"""Brokerless benchmark scenarios."""

from __future__ import absolute_import

import collections
import logging
import struct
import threading
import time

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion, _ConnectionState
from paho.mqtt.matcher import MQTTMatcher
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from fakes import FakeRecvSocket, FakeSendSocket, NonBlockingRecvSocket, make_out_packet, packet_deque
from micro_harness import Scenario


TOPIC = b"devices/device-0001/telemetry"
TOPIC_TEXT = TOPIC.decode("utf-8")
# Representative Zigbee2MQTT device topic (mqtt_zigbee_listener hot path).
Z2M_TOPIC = b"gw1/zigbee2mqtt/0x00158d0001234567"
Z2M_TOPIC_TEXT = Z2M_TOPIC.decode("utf-8")
PAYLOAD_SMALL = b'{"temperature":21.5,"humidity":44}'
PACK_U16 = struct.Struct("!H")

# Filters registered via @topic_callback in mqtt_zigbee_listener.py
Z2M_LISTENER_FILTERS = (
    "$SYS/broker/connection/+/state",
    "+/zigbee2mqtt/bridge/response/+",
    "+/zigbee2mqtt/bridge/+",
    "+/zigbee2mqtt/+",
    "+/modpoll/+/data",
    "+/terenergy/info",
    "+/terenergy/wifi",
)


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


def _pack_utf8(data):
    return PACK_U16.pack(len(data)) + data


def _publish_packet(protocol, payload, properties=None, qos=0, mid=1, topic=TOPIC):
    command = int(mqtt.PUBLISH) | (qos << 1)
    variable_header = bytearray()
    variable_header.extend(_pack_utf8(topic))
    if qos:
        variable_header.extend(PACK_U16.pack(mid))
    if protocol == mqtt.MQTTv5:
        if properties is None:
            variable_header.extend(b"\x00")
        else:
            variable_header.extend(properties.pack())
    remaining_length = len(variable_header) + len(payload)
    return bytes([command]) + _encode_varint(remaining_length) + bytes(variable_header) + payload


def _pubrel_packet(mid=1):
    # MQTT 3.1.1 PUBREL: command 0x62, remaining length 2, mid.
    return struct.pack("!BBH", int(mqtt.PUBREL) | 2, 2, mid)

def _common_properties():
    props = Properties(PacketTypes.PUBLISH)
    props.PayloadFormatIndicator = 1
    props.ContentType = "application/json"
    props.TopicAlias = 1
    return props


def _user_properties():
    props = Properties(PacketTypes.PUBLISH)
    for index in range(8):
        props.UserProperty = ("key{}".format(index), "value{}".format(index))
    return props


PACKED_EMPTY_PROPERTIES = Properties(PacketTypes.PUBLISH).pack()
PACKED_COMMON_PROPERTIES = _common_properties().pack()
PACKED_USER_PROPERTIES = _user_properties().pack()
PUBLISH_V3_QOS0_SMALL = _publish_packet(mqtt.MQTTv311, PAYLOAD_SMALL)
PUBLISH_V3_QOS0_LARGE = _publish_packet(mqtt.MQTTv311, b"x" * 65536)
PUBLISH_V5_QOS0_EMPTY_PROPS = _publish_packet(mqtt.MQTTv5, PAYLOAD_SMALL)
PUBLISH_V5_QOS0_USER_PROPS = _publish_packet(mqtt.MQTTv5, PAYLOAD_SMALL, _user_properties())
PUBLISH_V3_QOS2_SMALL = _publish_packet(mqtt.MQTTv311, PAYLOAD_SMALL, qos=2, mid=1)
PUBLISH_V3_QOS2_Z2M = _publish_packet(
    mqtt.MQTTv311, PAYLOAD_SMALL, qos=2, mid=1, topic=Z2M_TOPIC
)
PUBREL_MID1 = _pubrel_packet(1)
QOS2_CYCLE_SMALL = PUBLISH_V3_QOS2_SMALL + PUBREL_MID1
QOS2_CYCLE_Z2M = PUBLISH_V3_QOS2_Z2M + PUBREL_MID1


def _new_client(protocol=mqtt.MQTTv311):
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=protocol,
    )
    client.on_message = lambda client, userdata, message: None
    return client


def _new_recv_send_client(protocol=mqtt.MQTTv311):
    """Client with a socket that accepts both injected reads and ACK writes."""
    client = _new_client(protocol)
    # Placeholder; callers replace _sock with FakeRecvSocket(data).
    client._sock = FakeSendSocket()
    return client

def properties_pack_empty(iterations):
    props = Properties(PacketTypes.PUBLISH)
    for _ in range(iterations):
        props.pack()


def properties_unpack_empty(iterations):
    packed = PACKED_EMPTY_PROPERTIES
    for _ in range(iterations):
        props = Properties(PacketTypes.PUBLISH)
        props.unpack(packed)


def properties_pack_common(iterations):
    props = _common_properties()
    for _ in range(iterations):
        props.pack()


def properties_unpack_common(iterations):
    packed = PACKED_COMMON_PROPERTIES
    for _ in range(iterations):
        props = Properties(PacketTypes.PUBLISH)
        props.unpack(packed)


def properties_pack_user_properties(iterations):
    props = _user_properties()
    for _ in range(iterations):
        props.pack()


def properties_unpack_user_properties(iterations):
    packed = PACKED_USER_PROPERTIES
    for _ in range(iterations):
        props = Properties(PacketTypes.PUBLISH)
        props.unpack(packed)


def reasoncode_create_puback_success(iterations):
    for _ in range(iterations):
        ReasonCode(PacketTypes.PUBACK, identifier=0)


def _parse_publish(iterations, protocol, packet):
    client = _new_client(protocol)
    client._sock = FakeRecvSocket(packet * iterations)
    for _ in range(iterations):
        rc = client._packet_read()
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("packet read failed: {}".format(rc))


def publish_parse_v3_qos0_small(iterations):
    _parse_publish(iterations, mqtt.MQTTv311, PUBLISH_V3_QOS0_SMALL)


def publish_parse_v5_qos0_empty_props(iterations):
    _parse_publish(iterations, mqtt.MQTTv5, PUBLISH_V5_QOS0_EMPTY_PROPS)


def publish_parse_v5_qos0_user_props(iterations):
    _parse_publish(iterations, mqtt.MQTTv5, PUBLISH_V5_QOS0_USER_PROPS)


def publish_parse_v3_qos0_large(iterations):
    _parse_publish(iterations, mqtt.MQTTv311, PUBLISH_V3_QOS0_LARGE)


def loop_read_batch_v3_qos0_small(iterations):
    client = _new_client(mqtt.MQTTv311)
    delivered = [0]
    client.on_message = lambda *args: delivered.__setitem__(0, delivered[0] + 1)
    sock = NonBlockingRecvSocket(PUBLISH_V3_QOS0_SMALL * iterations)
    client._sock = sock
    while delivered[0] < iterations:
        rc = client._loop_read_batch(100)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("batch packet read failed: {}".format(rc))
    if sock.recv_calls >= iterations:
        raise RuntimeError("read-ahead did not reduce recv calls")


def loop_read_public_v3_qos0_small(iterations):
    """Exercise the existing public max_packets argument on a QoS 0 burst."""
    client = _new_client(mqtt.MQTTv311)
    delivered = [0]
    client.on_message = lambda *args: delivered.__setitem__(0, delivered[0] + 1)
    sock = NonBlockingRecvSocket(PUBLISH_V3_QOS0_SMALL * iterations)
    client._sock = sock
    loop_read_calls = 0
    while delivered[0] < iterations:
        rc = client.loop_read(100)
        loop_read_calls += 1
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("public loop_read failed: {}".format(rc))
    if loop_read_calls > iterations:
        raise RuntimeError("loop_read made no delivery progress")


def _parse_qos2_cycle(iterations, packet_cycle, register_z2m_filters=False):
    """PUBLISH QoS2 + PUBREL inbound cycle (PUBREC/PUBCOMP written to fake sock)."""
    client = _new_recv_send_client(mqtt.MQTTv311)
    if register_z2m_filters:
        for topic_filter in Z2M_LISTENER_FILTERS:
            client.message_callback_add(topic_filter, lambda *args: None)
    client._sock = FakeRecvSocket(packet_cycle * iterations)
    for _ in range(iterations):
        # PUBLISH qos2 -> store + PUBREC
        rc = client._packet_read()
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("qos2 publish read failed: {}".format(rc))
        # PUBREL -> dispatch + PUBCOMP
        rc = client._packet_read()
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("qos2 pubrel read failed: {}".format(rc))
    if client._in_messages:
        raise RuntimeError("expected empty _in_messages, got {}".format(len(client._in_messages)))


def publish_parse_v3_qos2_small(iterations):
    _parse_qos2_cycle(iterations, QOS2_CYCLE_SMALL)


def publish_parse_v3_qos2_z2m_filters(iterations):
    """Closest brokerless stand-in for mqtt_zigbee_listener receive path."""
    _parse_qos2_cycle(iterations, QOS2_CYCLE_Z2M, register_z2m_filters=True)


def publish_pack_qos0_v3_small(iterations):
    client = _new_client(mqtt.MQTTv311)
    client._sock = FakeSendSocket()
    for _ in range(iterations):
        info = client.publish(TOPIC_TEXT, PAYLOAD_SMALL, qos=0)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("publish failed: {}".format(info.rc))


def publish_pack_qos1_v3_small(iterations):
    client = _new_client(mqtt.MQTTv311)
    client._sock = FakeSendSocket()
    for mid in range(1, iterations + 1):
        rc = client._send_publish(mid, TOPIC, PAYLOAD_SMALL, qos=1)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("send publish failed: {}".format(rc))


def _publish_wire_packet(mid=1, qos=0):
    info = mqtt.MQTTMessageInfo(mid) if qos == 0 else None
    packet = bytearray()
    packet.append(int(mqtt.PUBLISH) | (qos << 1))
    remaining_length = 2 + len(TOPIC) + len(PAYLOAD_SMALL)
    if qos:
        remaining_length += 2
    packet.extend(_encode_varint(remaining_length))
    packet.extend(_pack_utf8(TOPIC))
    if qos:
        packet.extend(PACK_U16.pack(mid))
    packet.extend(PAYLOAD_SMALL)
    return make_out_packet(mqtt.PUBLISH, packet, mid=mid, qos=qos, info=info)


def _packet_write_drain(iterations, packets_per_iteration):
    template_packets = [_publish_wire_packet(mid=index + 1) for index in range(packets_per_iteration)]
    client = _new_client(mqtt.MQTTv311)
    client._sock = FakeSendSocket()
    for _ in range(iterations):
        packets = []
        for packet in template_packets:
            copied = dict(packet)
            copied["pos"] = 0
            copied["to_process"] = len(copied["packet"])
            if copied["info"] is not None:
                copied["info"] = mqtt.MQTTMessageInfo(copied["mid"])
            packets.append(copied)
        client._out_packet = packet_deque(packets)
        rc = client._packet_write()
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("packet write failed: {}".format(rc))


def packet_write_drain_100(iterations):
    _packet_write_drain(iterations, 100)


def packet_write_drain_10000(iterations):
    _packet_write_drain(iterations, 10000)


class _CountingSockpair(object):
    def __init__(self):
        self.sends = 0

    def send(self, data):
        self.sends += 1
        return len(data)

    def close(self):
        return None

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


def sockpair_wakeup_coalesce_10000(iterations):
    """Count sockpair wakeups while queuing 10000 packets with a live network thread."""
    client = _new_client(mqtt.MQTTv311)
    sockpair = _CountingSockpair()
    client._sockpairW = sockpair
    client._thread = threading.Thread(target=lambda: None)
    client._thread_terminate = True
    packet = b"x"
    for _ in range(iterations):
        sockpair.sends = 0
        with client._sockpair_wakeup_mutex:
            client._sockpair_wakeup_pending = False
        client._out_packet.clear()
        for mid in range(10000):
            rc = client._packet_queue(mqtt.PUBLISH, packet, mid, 0)
            if rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError("packet queue failed: {}".format(rc))
        if sockpair.sends != 1:
            raise RuntimeError("expected 1 wakeup, got {}".format(sockpair.sends))


class _DrainSockpair(_CountingSockpair):
    def __init__(self):
        super(_DrainSockpair, self).__init__()
        self._pending = 0

    def send(self, data):
        self.sends += 1
        self._pending += len(data)
        return len(data)

    def recv(self, size):
        if self._pending <= 0:
            raise BlockingIOError()
        n = min(size, self._pending)
        self._pending -= n
        return b"\x00" * n


def publish_threaded_qos0_v3_small(iterations):
    """Publish from the caller while a helper thread drains like loop_start()."""
    client = _new_client(mqtt.MQTTv311)
    client._sock = FakeSendSocket()
    client._state = _ConnectionState.MQTT_CS_CONNECTED
    sockpair = _DrainSockpair()
    client._sockpairW = sockpair
    client._sockpairR = sockpair
    # Force the threaded queue path (no immediate loop_write in _packet_queue).
    client._thread = threading.Thread(target=lambda: None)
    stop = threading.Event()

    def network_loop():
        while not stop.is_set():
            if client.want_write():
                client.loop_write()
                continue
            try:
                sockpair.recv(10000)
            except BlockingIOError:
                pass
            with client._sockpair_wakeup_mutex:
                client._sockpair_wakeup_pending = False
            stop.wait(0.00005)

    thread = threading.Thread(target=network_loop, name="bench-packet-write")
    thread.start()
    try:
        for _ in range(iterations):
            info = client.publish(TOPIC_TEXT, PAYLOAD_SMALL, qos=0)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                raise RuntimeError("publish failed: {}".format(info.rc))
        deadline = time.time() + 5.0
        while client.want_write() and time.time() < deadline:
            time.sleep(0.001)
        if client.want_write():
            raise RuntimeError("timed out waiting for packet drain")
    finally:
        stop.set()
        thread.join(2.0)


def matcher_many_filters(iterations):
    matcher = MQTTMatcher()
    for index in range(1000):
        matcher["devices/device-{}/telemetry".format(str(index).zfill(4))] = index
    matcher["devices/+/telemetry"] = "wildcard"
    matcher["devices/#"] = "all-devices"
    topic = TOPIC_TEXT
    for _ in range(iterations):
        list(matcher.iter_match(topic))


def _dispatch_client(filter_count):
    client = _new_client(mqtt.MQTTv311)
    for index in range(filter_count):
        if index == 0:
            client.message_callback_add("devices/+/telemetry", lambda *args: None)
        else:
            client.message_callback_add(
                "other/device-{}/x".format(index),
                lambda *args: None,
            )
    return client


def _dispatch_message():
    message = mqtt.MQTTMessage(create_info=False)
    message.topic = TOPIC
    message.payload = PAYLOAD_SMALL
    return message


def dispatch_no_filters(iterations):
    client = _dispatch_client(0)
    message = _dispatch_message()
    for _ in range(iterations):
        client._handle_on_message(message)


def dispatch_one_filter(iterations):
    client = _dispatch_client(1)
    message = _dispatch_message()
    for _ in range(iterations):
        client._handle_on_message(message)


def dispatch_many_filters(iterations):
    client = _dispatch_client(1000)
    message = _dispatch_message()
    for _ in range(iterations):
        client._handle_on_message(message)


def _dispatch_z2m_message():
    message = mqtt.MQTTMessage(create_info=False)
    message.topic = Z2M_TOPIC
    message.payload = PAYLOAD_SMALL
    return message


def dispatch_z2m_seven_filters(iterations):
    """Dispatch with the 7 topic_callback filters from mqtt_zigbee_listener.

    Callback reads message.topic (as the real listener does before Queue.put).
    """
    client = _new_client(mqtt.MQTTv311)

    def _cb(client, userdata, message):
        _ = message.topic

    for topic_filter in Z2M_LISTENER_FILTERS:
        client.message_callback_add(topic_filter, _cb)
    message = _dispatch_z2m_message()
    for _ in range(iterations):
        # Clear cache so each iteration pays first-access cost like a new message.
        if hasattr(message, "_topic_str"):
            message._topic_str = None
        client._handle_on_message(message)


def logging_disabled(iterations):
    client = _new_client(mqtt.MQTTv311)
    client.logger = logging.getLogger("paho.benchmark.disabled")
    client.logger.setLevel(logging.WARNING)
    for _ in range(iterations):
        client._easy_log(mqtt.MQTT_LOG_DEBUG, "Benchmark log message %d", 1)


def puback_qos1_no_callback(iterations):
    client = _new_client(mqtt.MQTTv311)
    client.on_publish = None
    client._max_inflight_messages = 0
    packets = bytearray()
    for mid in range(1, iterations + 1):
        message = mqtt.MQTTMessage(mid=mid, topic=TOPIC)
        message.qos = 1
        message.state = mqtt.mqtt_ms_wait_for_puback
        client._out_messages[mid] = message
        packets.extend(struct.pack("!BBH", int(mqtt.PUBACK), 2, mid))
    client._inflight_messages = iterations
    client._sock = FakeRecvSocket(packets)
    for _ in range(iterations):
        rc = client._packet_read()
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("PUBACK parse failed: {}".format(rc))


def _new_puback_refill_client(total_messages=1000, ack_count=100):
    client = _new_client(mqtt.MQTTv311)
    client.on_publish = None
    client._max_inflight_messages = ack_count
    client._inflight_messages = ack_count
    # Model loop_start(): generated PUBLISH packets stay queued until the read
    # batch returns to the network loop's write phase.
    client._thread = threading.Thread(target=lambda: None)
    packets = bytearray()
    for mid in range(1, total_messages + 1):
        message = mqtt.MQTTMessage(mid=mid, topic=TOPIC)
        message.payload = PAYLOAD_SMALL
        message.qos = 1
        message.state = (
            mqtt.mqtt_ms_wait_for_puback
            if mid <= ack_count
            else mqtt.mqtt_ms_queued
        )
        client._out_messages[mid] = message
        if mid <= ack_count:
            packets.extend(struct.pack("!BBH", int(mqtt.PUBACK), 2, mid))
    client._sock = NonBlockingRecvSocket(packets)
    return client


def puback_batch_refill_qos1(iterations):
    for _ in range(iterations):
        client = _new_puback_refill_client()
        rc = client._loop_read_batch(100)
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("PUBACK batch failed: {}".format(rc))
        if client._inflight_messages != 100:
            raise RuntimeError("inflight window was not refilled")
        if len(client._out_messages) != 900:
            raise RuntimeError("unexpected outgoing message count")
        if len(client._out_packet) != 100:
            raise RuntimeError("expected 100 promoted PUBLISH packets")


def reconnect_reset_qos2_1000(iterations):
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="benchmark-reconnect",
        clean_session=False,
    )
    for mid in range(1, 1001):
        message = mqtt.MQTTMessage(mid=mid, topic=TOPIC)
        message.qos = 2
        message.state = mqtt.mqtt_ms_wait_for_pubrec
        client._out_messages[mid] = message
    for _ in range(iterations):
        client._messages_reconnect_reset_out()


def _new_reconnect_replay_client(message_count=1000, qos=1):
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="benchmark-replay",
        clean_session=False,
    )
    client._sock = FakeSendSocket()
    client._max_inflight_messages = message_count
    for mid in range(1, message_count + 1):
        message = mqtt.MQTTMessage(mid=mid, topic=TOPIC)
        message.payload = PAYLOAD_SMALL
        message.qos = qos
        message.state = mqtt.mqtt_ms_publish
        message.dup = True
        client._out_messages[mid] = message
    client._in_packet.packet = bytearray((0, 0))
    client._in_packet.remaining_length = 2
    client._replay_loop_write_calls = 0
    real_loop_write = client.loop_write

    def counting_loop_write():
        client._replay_loop_write_calls += 1
        return real_loop_write()

    client.loop_write = counting_loop_write
    return client


def reconnect_replay_qos1_1000(iterations):
    for _ in range(iterations):
        client = _new_reconnect_replay_client()
        rc = client._handle_connack()
        if rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError("CONNACK replay failed: {}".format(rc))
        if client._inflight_messages != 1000 or client._out_packet:
            raise RuntimeError("replay did not drain all eligible messages")
        if client._replay_loop_write_calls != 16:
            raise RuntimeError("expected 16 bounded replay drains, got {}".format(client._replay_loop_write_calls))


def _websocket_frame(iterations, size):
    wrapper = mqtt._WebsocketWrapper.__new__(mqtt._WebsocketWrapper)
    for _ in range(iterations):
        wrapper._create_frame(mqtt._WebsocketWrapper.OPCODE_BINARY, bytearray(b"x" * size))


def websocket_frame_16(iterations):
    _websocket_frame(iterations, 16)


def websocket_frame_128(iterations):
    _websocket_frame(iterations, 128)


def websocket_frame_1024(iterations):
    _websocket_frame(iterations, 1024)


SCENARIOS = [
    Scenario("properties_pack_empty", "mqttv5-codec", "property-set", 5000, properties_pack_empty),
    Scenario("properties_unpack_empty", "mqttv5-codec", "property-set", 5000, properties_unpack_empty),
    Scenario("properties_pack_common", "mqttv5-codec", "property-set", 3000, properties_pack_common),
    Scenario("properties_unpack_common", "mqttv5-codec", "property-set", 3000, properties_unpack_common),
    Scenario("properties_pack_user_properties", "mqttv5-codec", "property-set", 1000, properties_pack_user_properties),
    Scenario("properties_unpack_user_properties", "mqttv5-codec", "property-set", 1000, properties_unpack_user_properties),
    Scenario("reasoncode_create_puback_success", "mqttv5-codec", "reason-code", 5000, reasoncode_create_puback_success),
    Scenario("publish_parse_v3_qos0_small", "packet-read", "message", 5000, publish_parse_v3_qos0_small),
    Scenario("publish_parse_v3_qos0_large", "packet-read", "message", 100, publish_parse_v3_qos0_large),
    Scenario("loop_read_batch_v3_qos0_small", "packet-read", "message", 5000, loop_read_batch_v3_qos0_small),
    Scenario("loop_read_public_v3_qos0_small", "packet-read", "message", 5000, loop_read_public_v3_qos0_small),
    Scenario("publish_parse_v5_qos0_empty_props", "packet-read", "message", 5000, publish_parse_v5_qos0_empty_props),
    Scenario("publish_parse_v5_qos0_user_props", "packet-read", "message", 1000, publish_parse_v5_qos0_user_props),
    Scenario("publish_parse_v3_qos2_small", "packet-read", "message", 3000, publish_parse_v3_qos2_small),
    Scenario(
        "publish_parse_v3_qos2_z2m_filters",
        "packet-read",
        "message",
        3000,
        publish_parse_v3_qos2_z2m_filters,
    ),
    Scenario("publish_pack_qos0_v3_small", "packet-write", "message", 5000, publish_pack_qos0_v3_small),
    Scenario("publish_pack_qos1_v3_small", "packet-write", "message", 5000, publish_pack_qos1_v3_small),
    Scenario("publish_threaded_qos0_v3_small", "packet-write", "message", 3000, publish_threaded_qos0_v3_small),
    Scenario("packet_write_drain_100", "packet-write", "packet", 100, packet_write_drain_100, operations_per_iteration=100),
    Scenario("packet_write_drain_10000", "packet-write", "packet", 1, packet_write_drain_10000, operations_per_iteration=10000),
    Scenario("sockpair_wakeup_coalesce_10000", "packet-write", "wakeup", 20, sockpair_wakeup_coalesce_10000, operations_per_iteration=10000),
    Scenario("matcher_many_filters", "supporting", "match", 2000, matcher_many_filters),
    Scenario("dispatch_no_filters", "callback-dispatch", "message", 10000, dispatch_no_filters),
    Scenario("dispatch_one_filter", "callback-dispatch", "message", 5000, dispatch_one_filter),
    Scenario("dispatch_many_filters", "callback-dispatch", "message", 3000, dispatch_many_filters),
    Scenario("dispatch_z2m_seven_filters", "callback-dispatch", "message", 5000, dispatch_z2m_seven_filters),
    Scenario("logging_disabled", "supporting", "log-call", 20000, logging_disabled),
    Scenario("puback_qos1_no_callback", "ack-completion", "ack", 3000, puback_qos1_no_callback),
    Scenario("puback_batch_refill_qos1", "ack-completion", "ack", 20, puback_batch_refill_qos1, operations_per_iteration=100),
    Scenario("reconnect_reset_qos2_1000", "reconnect", "reset", 100, reconnect_reset_qos2_1000),
    Scenario("reconnect_replay_qos1_1000", "reconnect", "replay", 20, reconnect_replay_qos1_1000),
    Scenario("websocket_frame_16", "websocket", "frame", 10000, websocket_frame_16),
    Scenario("websocket_frame_128", "websocket", "frame", 5000, websocket_frame_128),
    Scenario("websocket_frame_1024", "websocket", "frame", 1000, websocket_frame_1024),
]
