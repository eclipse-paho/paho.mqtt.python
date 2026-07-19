import threading
import time
import unicodedata

import paho.mqtt.client as client
import pytest
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode, MQTTProtocolVersion
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

import tests.paho_test as paho_test

# Import test fixture
from tests.testsupport.broker import FakeBroker, fake_broker  # noqa: F401


@pytest.mark.parametrize("proto_ver,callback_version", [
    (MQTTProtocolVersion.MQTTv31, CallbackAPIVersion.VERSION1),
    (MQTTProtocolVersion.MQTTv31, CallbackAPIVersion.VERSION2),
    (MQTTProtocolVersion.MQTTv311, CallbackAPIVersion.VERSION1),
    (MQTTProtocolVersion.MQTTv311, CallbackAPIVersion.VERSION2),
])
class Test_connect:
    """
    Tests on connect/disconnect behaviour of the client
    """

    def test_01_con_discon_success(self, proto_ver, callback_version, fake_broker):
        mqttc = client.Client(
            callback_version,
            "01-con-discon-success",
            protocol=proto_ver,
            transport=fake_broker.transport,
        )

        def on_connect(mqttc, obj, flags, rc_or_reason_code, properties_or_none=None):
            assert rc_or_reason_code == 0
            mqttc.disconnect()

        mqttc.on_connect = on_connect

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "01-con-discon-success", keepalive=60,
                proto_ver=proto_ver)
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            disconnect_packet = paho_test.gen_disconnect()
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == disconnect_packet

        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_01_con_failure_rc(self, proto_ver, callback_version, fake_broker):
        mqttc = client.Client(
            callback_version, "01-con-failure-rc",
            protocol=proto_ver, transport=fake_broker.transport)

        def on_connect(mqttc, obj, flags, rc_or_reason_code, properties_or_none=None):
            assert rc_or_reason_code > 0
            assert rc_or_reason_code != 0
            if callback_version == CallbackAPIVersion.VERSION1:
                assert rc_or_reason_code == 1
            else:
                assert rc_or_reason_code == ReasonCode(PacketTypes.CONNACK, "Unsupported protocol version")

        mqttc.on_connect = on_connect

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "01-con-failure-rc", keepalive=60,
                proto_ver=proto_ver)
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=1)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            packet_in = fake_broker.receive_packet(1)
            assert not packet_in  # Check connection is closed

        finally:
            mqttc.loop_stop()

    def test_connection_properties(self, proto_ver, callback_version, fake_broker):
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2, "client-id",
            protocol=proto_ver, transport=fake_broker.transport)
        mqttc.enable_logger()

        is_connected = threading.Event()
        is_disconnected = threading.Event()

        def on_connect(mqttc, obj, flags, rc, properties):
            assert rc == 0
            is_connected.set()

        def on_disconnect(*args):
            import logging
            logging.info("disco")
            is_disconnected.set()

        mqttc.on_connect = on_connect
        mqttc.on_disconnect = on_disconnect

        mqttc.host = "localhost"
        mqttc.connect_timeout = 7
        mqttc.port = fake_broker.port
        mqttc.keepalive = 7
        mqttc.max_inflight_messages = 7
        mqttc.max_queued_messages = 7
        mqttc.transport = fake_broker.transport
        mqttc.username = "username"
        mqttc.password = "password"

        mqttc.reconnect()

        # As soon as connection try to be established, no longer accept updates
        with pytest.raises(RuntimeError):
            mqttc.host = "localhost"

        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "client-id",
                keepalive=7,
                username="username",
                password="password",
                proto_ver=proto_ver,
            )
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            is_connected.wait()

            # Check that all connections related properties can't be updated
            with pytest.raises(RuntimeError):
                mqttc.host = "localhost"

            with pytest.raises(RuntimeError):
                mqttc.connect_timeout = 7

            with pytest.raises(RuntimeError):
                mqttc.port = fake_broker.port

            with pytest.raises(RuntimeError):
                mqttc.keepalive = 7

            with pytest.raises(RuntimeError):
                mqttc.max_inflight_messages = 7

            with pytest.raises(RuntimeError):
                mqttc.max_queued_messages = 7

            with pytest.raises(RuntimeError):
                mqttc.transport = fake_broker.transport

            with pytest.raises(RuntimeError):
                mqttc.username = "username"

            with pytest.raises(RuntimeError):
                mqttc.password = "password"

            # close the connection, but from broker
            fake_broker.finish()

            is_disconnected.wait()
            assert not mqttc.is_connected()

            # still not allowed to update, because client try to reconnect in background
            with pytest.raises(RuntimeError):
                mqttc.host = "localhost"

            mqttc.disconnect()

            # Now it's allowed, connection is closing AND not trying to reconnect
            mqttc.host = "localhost"

        finally:
            mqttc.loop_stop()


class Test_connect_v5:
    """
    Tests on connect/disconnect behaviour of the client with MQTTv5
    """

    def test_01_broker_no_support(self, fake_broker):
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2, "01-broker-no-support",
            protocol=MQTTProtocolVersion.MQTTv5, transport=fake_broker.transport)

        def on_connect(mqttc, obj, flags, reason, properties):
            assert reason == 132
            assert reason == ReasonCode(client.CONNACK >> 4, aName="Unsupported protocol version")
            mqttc.disconnect()

        mqttc.on_connect = on_connect

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            # Can't test the connect_packet, we can't yet generate MQTTv5 packet.
            # connect_packet = paho_test.gen_connect(
            #     "01-con-discon-success", keepalive=60,
            #     proto_ver=client.MQTTv311)
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            # assert packet_in == connect_packet

            # The reply packet is a MQTTv3 connack. But that the propose of this test,
            # ensure client convert it to a reason code 132 "Unsupported protocol version"
            connack_packet = paho_test.gen_connack(rc=1)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            packet_in = fake_broker.receive_packet(1)
            assert not packet_in  # Check connection is closed

        finally:
            mqttc.loop_stop()


class TestConnectionLost:
    def test_with_loop_start(self, fake_broker: FakeBroker):
        mqttc = client.Client(
            CallbackAPIVersion.VERSION1,
            "test_with_loop_start",
            protocol=MQTTProtocolVersion.MQTTv311,
            reconnect_on_failure=False,
            transport=fake_broker.transport
        )

        on_connect_reached = threading.Event()
        on_disconnect_reached = threading.Event()


        def on_connect(mqttc, obj, flags, rc):
            assert rc == 0
            on_connect_reached.set()

        def on_disconnect(*args):
            on_disconnect_reached.set()

        mqttc.on_connect = on_connect
        mqttc.on_disconnect = on_disconnect

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "test_with_loop_start", keepalive=60,
                proto_ver=MQTTProtocolVersion.MQTTv311)
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            assert on_connect_reached.wait(1)
            assert mqttc.is_connected()

            fake_broker.finish()

            assert on_disconnect_reached.wait(1)
            assert not mqttc.is_connected()

        finally:
            mqttc.loop_stop()

    def test_with_loop(self, fake_broker: FakeBroker):
        mqttc = client.Client(
            CallbackAPIVersion.VERSION1,
            "test_with_loop",
            clean_session=True,
            transport=fake_broker.transport,
        )

        on_connect_reached = threading.Event()
        on_disconnect_reached = threading.Event()


        def on_connect(mqttc, obj, flags, rc):
            assert rc == 0
            on_connect_reached.set()

        def on_disconnect(*args):
            on_disconnect_reached.set()

        mqttc.on_connect = on_connect
        mqttc.on_disconnect = on_disconnect

        mqttc.connect("localhost", fake_broker.port)

        fake_broker.start()

        # not yet connected, packet are not yet processed by loop()
        assert not mqttc.is_connected()

        # connect packet is sent during connect() call
        connect_packet = paho_test.gen_connect(
            "test_with_loop", keepalive=60,
            proto_ver=MQTTProtocolVersion.MQTTv311)
        packet_in = fake_broker.receive_packet(1000)
        assert packet_in  # Check connection was not closed
        assert packet_in == connect_packet

        connack_packet = paho_test.gen_connack(rc=0)
        count = fake_broker.send_packet(connack_packet)
        assert count  # Check connection was not closed
        assert count == len(connack_packet)

        # call loop() to process the connack packet
        assert mqttc.loop(timeout=1) == MQTTErrorCode.MQTT_ERR_SUCCESS

        assert on_connect_reached.wait(1)
        assert mqttc.is_connected()

        fake_broker.finish()

        # call loop() to detect the connection lost
        assert mqttc.loop(timeout=1) == MQTTErrorCode.MQTT_ERR_CONN_LOST

        assert on_disconnect_reached.wait(1)
        assert not mqttc.is_connected()


class TestPublish:
    def test_publish_before_connect(self, fake_broker: FakeBroker) -> None:
        mqttc = client.Client(
            CallbackAPIVersion.VERSION1,
            "test_publish_before_connect",
            transport=fake_broker.transport,
        )

        def on_connect(mqttc, obj, flags, rc):
            assert rc == 0

        mqttc.on_connect = on_connect

        mqttc.loop_start()
        mqttc.connect("localhost", fake_broker.port)
        mqttc.enable_logger()

        try:
            mi = mqttc.publish("test", "testing")

            fake_broker.start()

            packet_in = fake_broker.receive_packet(1)
            assert not packet_in  # Check connection is closed
            # re-call fake_broker.start() to take the 2nd connection done by client
            # ... this is probably a bug, when using loop_start/loop_forever
            # and doing a connect() before, the TCP connection is opened twice.
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "test_publish_before_connect", keepalive=60,
                proto_ver=client.MQTTv311)
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            with pytest.raises(RuntimeError):
                mi.wait_for_publish(1)

            mqttc.disconnect()

            disconnect_packet = paho_test.gen_disconnect()
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == disconnect_packet

        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    @pytest.mark.parametrize("user_payload,sent_payload", [
        ("string", b"string"),
        (b"byte", b"byte"),
        (bytearray(b"bytearray"), b"bytearray"),
        (42, b"42"),
        (4.2, b"4.2"),
        (None, b""),
    ])
    def test_publish_various_payload(self, user_payload: client.PayloadType, sent_payload: bytes, fake_broker: FakeBroker) -> None:
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "test_publish_various_payload",
            transport=fake_broker.transport,
        )

        mqttc.connect("localhost", fake_broker.port)
        mqttc.loop_start()
        mqttc.enable_logger()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "test_publish_various_payload", keepalive=60,
                proto_ver=client.MQTTv311)
            fake_broker.expect_packet("connect", connect_packet)

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            mqttc.publish("test", user_payload)

            publish_packet = paho_test.gen_publish(
                b"test", payload=sent_payload, qos=0
            )
            fake_broker.expect_packet("publish", publish_packet)

            mqttc.disconnect()

            disconnect_packet = paho_test.gen_disconnect()
            packet_in = fake_broker.receive_packet(1000)
            assert packet_in  # Check connection was not closed
            assert packet_in == disconnect_packet

        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed


@pytest.mark.parametrize("callback_version", [
    (CallbackAPIVersion.VERSION1),
    (CallbackAPIVersion.VERSION2),
])
class TestPublishBroker2Client:
    def test_invalid_utf8_topic(self, callback_version, fake_broker):
        mqttc = client.Client(callback_version, "client-id", transport=fake_broker.transport)

        def on_message(client, userdata, msg):
            with pytest.raises(UnicodeDecodeError):
                assert msg.topic
            client.disconnect()

        mqttc.on_message = on_message

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect("client-id")
            packet_in = fake_broker.receive_packet(len(connect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            publish_packet = paho_test.gen_publish(b"\xff", qos=0)
            count = fake_broker.send_packet(publish_packet)
            assert count  # Check connection was not closed
            assert count == len(publish_packet)

            disconnect_packet = paho_test.gen_disconnect()
            packet_in = fake_broker.receive_packet(len(disconnect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == disconnect_packet

        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_valid_utf8_topic_recv(self, callback_version, fake_broker):
        mqttc = client.Client(callback_version, "client-id", transport=fake_broker.transport)

        # It should be non-ascii multi-bytes character
        topic = unicodedata.lookup('SNOWMAN')

        def on_message(client, userdata, msg):
            assert msg.topic == topic
            client.disconnect()

        mqttc.on_message = on_message

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect("client-id")
            packet_in = fake_broker.receive_packet(len(connect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            publish_packet = paho_test.gen_publish(
                topic.encode('utf-8'), qos=0
            )
            count = fake_broker.send_packet(publish_packet)
            assert count  # Check connection was not closed
            assert count == len(publish_packet)

            disconnect_packet = paho_test.gen_disconnect()
            packet_in = fake_broker.receive_packet(len(disconnect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == disconnect_packet

        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_valid_utf8_topic_publish(self, callback_version, fake_broker):
        mqttc = client.Client(callback_version, "client-id", transport=fake_broker.transport)

        # It should be non-ascii multi-bytes character
        topic = unicodedata.lookup('SNOWMAN')

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect("client-id")
            packet_in = fake_broker.receive_packet(len(connect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            mqttc.publish(topic, None, 0)
            # Small sleep needed to avoid connection reset.
            time.sleep(0.3)

            publish_packet = paho_test.gen_publish(
                topic.encode('utf-8'), qos=0
            )
            packet_in = fake_broker.receive_packet(len(publish_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == publish_packet

            mqttc.disconnect()

            disconnect_packet = paho_test.gen_disconnect()
            packet_in = fake_broker.receive_packet(len(disconnect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == disconnect_packet

        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_message_callback(self, callback_version, fake_broker):
        mqttc = client.Client(callback_version, "client-id", transport=fake_broker.transport)
        userdata = {
            'on_message': 0,
            'callback1': 0,
            'callback2': 0,
        }
        mqttc.user_data_set(userdata)

        def on_message(client, userdata, msg):
            assert msg.topic == 'topic/value'
            userdata['on_message'] += 1

        def callback1(client, userdata, msg):
            assert msg.topic == 'topic/callback/1'
            userdata['callback1'] += 1

        def callback2(client, userdata, msg):
            assert msg.topic in ('topic/callback/3', 'topic/callback/1')
            userdata['callback2'] += 1

        mqttc.on_message = on_message
        mqttc.message_callback_add('topic/callback/1', callback1)
        mqttc.message_callback_add('topic/callback/+', callback2)

        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect("client-id")
            packet_in = fake_broker.receive_packet(len(connect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == connect_packet

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            publish_packet = paho_test.gen_publish(b"topic/value", qos=1, mid=1)
            count = fake_broker.send_packet(publish_packet)
            assert count  # Check connection was not closed
            assert count == len(publish_packet)

            publish_packet = paho_test.gen_publish(b"topic/callback/1", qos=1, mid=2)
            count = fake_broker.send_packet(publish_packet)
            assert count  # Check connection was not closed
            assert count == len(publish_packet)

            publish_packet = paho_test.gen_publish(b"topic/callback/3", qos=1, mid=3)
            count = fake_broker.send_packet(publish_packet)
            assert count  # Check connection was not closed
            assert count == len(publish_packet)


            puback_packet = paho_test.gen_puback(mid=1)
            packet_in = fake_broker.receive_packet(len(puback_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == puback_packet

            puback_packet = paho_test.gen_puback(mid=2)
            packet_in = fake_broker.receive_packet(len(puback_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == puback_packet

            puback_packet = paho_test.gen_puback(mid=3)
            packet_in = fake_broker.receive_packet(len(puback_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == puback_packet

            mqttc.disconnect()

            disconnect_packet = paho_test.gen_disconnect()
            packet_in = fake_broker.receive_packet(len(disconnect_packet))
            assert packet_in  # Check connection was not closed
            assert packet_in == disconnect_packet

        finally:
            mqttc.loop_stop()

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

        assert userdata['on_message'] == 1
        assert userdata['callback1'] == 1
        assert userdata['callback2'] == 2


class TestCompatibility:
    """
    Some tests for backward compatibility
    """

    def test_change_error_code_to_enum(self):
        """Make sure code don't break after MQTTErrorCode enum introduction"""
        rc_ok = client.MQTTErrorCode.MQTT_ERR_SUCCESS
        rc_again = client.MQTTErrorCode.MQTT_ERR_AGAIN
        rc_err = client.MQTTErrorCode.MQTT_ERR_NOMEM

        # Access using old name still works
        assert rc_ok == client.MQTT_ERR_SUCCESS

        # User might compare to 0 to check for success
        assert rc_ok == 0
        assert not rc_err == 0
        assert not rc_again == 0
        assert not rc_ok != 0
        assert rc_err != 0
        assert rc_again != 0

        # User might compare to specific code
        assert rc_again == -1
        assert rc_err == 1

        # User might just use "if rc:"
        assert not rc_ok
        assert rc_err
        assert rc_again

        # User might do inequality with 0 (like "if rc > 0")
        assert not (rc_ok > 0)
        assert rc_err > 0
        assert rc_again < 0

        # This might probably not be done: User might use rc as number in
        # operation
        assert rc_ok + 1 == 1

    def test_migration_callback_version(self):
        with pytest.raises(ValueError, match="see docs/migrations.rst"):
            _ = client.Client("client-id")

    def test_callback_v1_mqtt3(self, fake_broker):
        callback_called = []
        with pytest.deprecated_call():
            mqttc = client.Client(
                CallbackAPIVersion.VERSION1,
                "client-id",
                userdata=callback_called,
                transport=fake_broker.transport,
            )

        def on_connect(cl, userdata, flags, rc):
            assert isinstance(cl, client.Client)
            assert isinstance(flags, dict)
            assert isinstance(flags["session present"], int)
            assert isinstance(rc, int)
            userdata.append("on_connect")
            cl.subscribe([("topic", 0)])

        def on_subscribe(cl, userdata, mid, granted_qos):
            assert isinstance(cl, client.Client)
            assert isinstance(mid, int)
            assert isinstance(granted_qos, tuple)
            assert isinstance(granted_qos[0], int)
            userdata.append("on_subscribe")
            cl.publish("topic", "payload", 2)

        def on_publish(cl, userdata, mid):
            assert isinstance(cl, client.Client)
            assert isinstance(mid, int)
            userdata.append("on_publish")

        def on_message(cl, userdata, message):
            assert isinstance(cl, client.Client)
            assert isinstance(message, client.MQTTMessage)
            userdata.append("on_message")
            cl.unsubscribe("topic")

        def on_unsubscribe(cl, userdata, mid):
            assert isinstance(cl, client.Client)
            assert isinstance(mid, int)
            userdata.append("on_unsubscribe")
            cl.disconnect()

        def on_disconnect(cl, userdata, rc):
            assert isinstance(cl, client.Client)
            assert isinstance(rc, int)
            userdata.append("on_disconnect")

        mqttc.on_connect = on_connect
        mqttc.on_subscribe = on_subscribe
        mqttc.on_publish = on_publish
        mqttc.on_message = on_message
        mqttc.on_unsubscribe = on_unsubscribe
        mqttc.on_disconnect = on_disconnect

        mqttc.enable_logger()
        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "client-id", keepalive=60)
            fake_broker.expect_packet("connect", connect_packet)

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            subscribe_packet = paho_test.gen_subscribe(1, "topic", 0)
            fake_broker.expect_packet("subscribe", subscribe_packet)

            suback_packet = paho_test.gen_suback(1, 0)
            count = fake_broker.send_packet(suback_packet)
            assert count  # Check connection was not closed
            assert count == len(suback_packet)

            publish_packet = paho_test.gen_publish("topic", 2, "payload", mid=2)
            fake_broker.expect_packet("publish", publish_packet)

            pubrec_packet = paho_test.gen_pubrec(mid=2)
            count = fake_broker.send_packet(pubrec_packet)
            assert count  # Check connection was not closed
            assert count == len(pubrec_packet)

            pubrel_packet = paho_test.gen_pubrel(mid=2)
            fake_broker.expect_packet("pubrel", pubrel_packet)

            pubcomp_packet = paho_test.gen_pubcomp(mid=2)
            count = fake_broker.send_packet(pubcomp_packet)
            assert count  # Check connection was not closed
            assert count == len(pubcomp_packet)

            publish_from_broker_packet = paho_test.gen_publish("topic", qos=0, payload="payload", mid=99)
            count = fake_broker.send_packet(publish_from_broker_packet)
            assert count  # Check connection was not closed
            assert count == len(publish_from_broker_packet)

            unsubscribe_packet = paho_test.gen_unsubscribe(mid=3, topic="topic")
            fake_broker.expect_packet("unsubscribe", unsubscribe_packet)

            suback_packet = paho_test.gen_unsuback(mid=3)
            count = fake_broker.send_packet(suback_packet)
            assert count  # Check connection was not closed
            assert count == len(suback_packet)

            disconnect_packet = paho_test.gen_disconnect()
            fake_broker.expect_packet("disconnect", disconnect_packet)

        finally:
            mqttc.disconnect()
            mqttc.loop_stop()

        assert callback_called == [
            "on_connect",
            "on_subscribe",
            "on_publish",
            "on_message",
            "on_unsubscribe",
            "on_disconnect",
        ]

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed

    def test_callback_v2_mqtt3(self, fake_broker):
        callback_called = []
        mqttc = client.Client(
            CallbackAPIVersion.VERSION2,
            "client-id",
            userdata=callback_called,
            transport=fake_broker.transport,
        )

        def on_connect(cl, userdata, flags, reason, properties):
            assert isinstance(cl, client.Client)
            assert isinstance(flags, client.ConnectFlags)
            assert isinstance(reason, ReasonCode)
            assert isinstance(properties, Properties)
            assert reason == 0
            assert properties.isEmpty()
            userdata.append("on_connect")
            cl.subscribe([("topic", 0)])

        def on_subscribe(cl, userdata, mid, reason_code_list, properties):
            assert isinstance(cl, client.Client)
            assert isinstance(mid, int)
            assert isinstance(reason_code_list, list)
            assert isinstance(reason_code_list[0], ReasonCode)
            assert isinstance(properties, Properties)
            assert properties.isEmpty()
            userdata.append("on_subscribe")
            cl.publish("topic", "payload", 2)

        def on_publish(cl, userdata, mid, reason_code, properties):
            assert isinstance(cl, client.Client)
            assert isinstance(mid, int)
            assert isinstance(reason_code, ReasonCode)
            assert isinstance(properties, Properties)
            assert properties.isEmpty()
            userdata.append("on_publish")

        def on_message(cl, userdata, message):
            assert isinstance(cl, client.Client)
            assert isinstance(message, client.MQTTMessage)
            userdata.append("on_message")
            cl.unsubscribe("topic")

        def on_unsubscribe(cl, userdata, mid, reason_code_list, properties):
            assert isinstance(cl, client.Client)
            assert isinstance(mid, int)
            assert isinstance(reason_code_list, list)
            assert len(reason_code_list) == 0
            assert isinstance(properties, Properties)
            assert properties.isEmpty()
            userdata.append("on_unsubscribe")
            cl.disconnect()

        def on_disconnect(cl, userdata, flags, reason_code, properties):
            assert isinstance(cl, client.Client)
            assert isinstance(flags, client.DisconnectFlags)
            assert isinstance(reason_code, ReasonCode)
            assert isinstance(properties, Properties)
            assert properties.isEmpty()
            userdata.append("on_disconnect")

        mqttc.on_connect = on_connect
        mqttc.on_subscribe = on_subscribe
        mqttc.on_publish = on_publish
        mqttc.on_message = on_message
        mqttc.on_unsubscribe = on_unsubscribe
        mqttc.on_disconnect = on_disconnect

        mqttc.enable_logger()
        mqttc.connect_async("localhost", fake_broker.port)
        mqttc.loop_start()

        try:
            fake_broker.start()

            connect_packet = paho_test.gen_connect(
                "client-id", keepalive=60)
            fake_broker.expect_packet("connect", connect_packet)

            connack_packet = paho_test.gen_connack(rc=0)
            count = fake_broker.send_packet(connack_packet)
            assert count  # Check connection was not closed
            assert count == len(connack_packet)

            subscribe_packet = paho_test.gen_subscribe(1, "topic", 0)
            fake_broker.expect_packet("subscribe", subscribe_packet)

            suback_packet = paho_test.gen_suback(1, 0)
            count = fake_broker.send_packet(suback_packet)
            assert count  # Check connection was not closed
            assert count == len(suback_packet)

            publish_packet = paho_test.gen_publish("topic", 2, "payload", mid=2)
            fake_broker.expect_packet("publish", publish_packet)

            pubrec_packet = paho_test.gen_pubrec(mid=2)
            count = fake_broker.send_packet(pubrec_packet)
            assert count  # Check connection was not closed
            assert count == len(pubrec_packet)

            pubrel_packet = paho_test.gen_pubrel(mid=2)
            fake_broker.expect_packet("pubrel", pubrel_packet)

            pubcomp_packet = paho_test.gen_pubcomp(mid=2)
            count = fake_broker.send_packet(pubcomp_packet)
            assert count  # Check connection was not closed
            assert count == len(pubcomp_packet)

            publish_from_broker_packet = paho_test.gen_publish("topic", qos=0, payload="payload", mid=99)
            count = fake_broker.send_packet(publish_from_broker_packet)
            assert count  # Check connection was not closed
            assert count == len(publish_from_broker_packet)

            unsubscribe_packet = paho_test.gen_unsubscribe(mid=3, topic="topic")
            fake_broker.expect_packet("unsubscribe", unsubscribe_packet)

            suback_packet = paho_test.gen_unsuback(mid=3)
            count = fake_broker.send_packet(suback_packet)
            assert count  # Check connection was not closed
            assert count == len(suback_packet)

            disconnect_packet = paho_test.gen_disconnect()
            fake_broker.expect_packet("disconnect", disconnect_packet)

        finally:
            mqttc.disconnect()
            mqttc.loop_stop()

        assert callback_called == [
            "on_connect",
            "on_subscribe",
            "on_publish",
            "on_message",
            "on_unsubscribe",
            "on_disconnect",
        ]

        packet_in = fake_broker.receive_packet(1)
        assert not packet_in  # Check connection is closed
