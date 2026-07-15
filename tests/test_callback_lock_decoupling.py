import struct
import threading

import pytest
from paho.mqtt import client
from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion


class QueueOnlySocket:
    def send(self, data):
        return len(data)

    def close(self):
        return None


def _outgoing_client(mid=7):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = QueueOnlySocket()
    mqttc._max_inflight_messages = 1
    mqttc._inflight_messages = 1
    message = client.MQTTMessage(mid=mid, topic=b"audit/out")
    message.qos = 1
    message.state = client.mqtt_ms_wait_for_puback
    mqttc._out_messages[mid] = message
    mqttc._in_packet.remaining_length = 2
    mqttc._in_packet.packet = struct.pack("!H", mid)
    return mqttc, message


def _run_in_thread(function):
    result = []
    error = []

    def target():
        try:
            result.append(function())
        except BaseException as err:  # propagate worker failures to the test
            error.append(err)

    thread = threading.Thread(target=target)
    thread.start()
    return thread, result, error


def test_on_publish_releases_state_lock_but_reserves_inflight_slot():
    mqttc, completed = _outgoing_client()
    entered = threading.Event()
    release = threading.Event()

    def on_publish(mqttc, userdata, mid, reason_code, properties):
        assert completed.info.is_published() is False
        entered.set()
        assert release.wait(2.0)

    mqttc.on_publish = on_publish
    ack_thread, ack_result, ack_error = _run_in_thread(
        lambda: mqttc._handle_pubackcomp("PUBACK")
    )
    mqttc._thread = ack_thread
    assert entered.wait(1.0)

    published = []
    producer_thread, _result, producer_error = _run_in_thread(
        lambda: published.append(mqttc.publish("audit/next", b"x", qos=1))
    )
    assert producer_thread.join(0.25) is None
    assert not producer_thread.is_alive(), "publish() waited for the user callback"
    assert producer_error == []
    assert len(published) == 1
    next_message = mqttc._out_messages[published[0].mid]
    assert next_message.state == client.mqtt_ms_queued
    assert mqttc._inflight_messages == 1
    assert completed.info.is_published() is False

    release.set()
    ack_thread.join(1.0)
    assert not ack_thread.is_alive()
    assert ack_error == []
    assert ack_result == [client.MQTT_ERR_SUCCESS]
    assert completed.info.is_published() is True
    assert completed.mid not in mqttc._out_messages
    assert next_message.state == client.mqtt_ms_wait_for_puback
    assert mqttc._inflight_messages == 1


def test_duplicate_puback_during_callback_is_ignored():
    mqttc, message = _outgoing_client()
    callbacks = []

    def on_publish(mqttc, userdata, mid, reason_code, properties):
        callbacks.append(mid)
        mqttc._in_packet.remaining_length = 2
        mqttc._in_packet.packet = struct.pack("!H", mid)
        assert mqttc._handle_pubackcomp("PUBACK") == client.MQTT_ERR_SUCCESS

    mqttc.on_publish = on_publish
    assert mqttc._handle_pubackcomp("PUBACK") == client.MQTT_ERR_SUCCESS
    assert callbacks == [message.mid]
    assert message.info.is_published() is True
    assert mqttc._out_messages == {}

    # A late duplicate remains ignored after callback removal.
    assert mqttc._handle_pubackcomp("PUBACK") == client.MQTT_ERR_SUCCESS
    assert callbacks == [message.mid]
    assert message.info.is_published() is True
    assert mqttc._out_messages == {}
    assert mqttc._inflight_messages == 0


def test_unsuppressed_on_publish_exception_leaves_ack_recoverable():
    mqttc, message = _outgoing_client()

    def fail(*args):
        raise RuntimeError("callback failed")

    mqttc.on_publish = fail
    with pytest.raises(RuntimeError, match="callback failed"):
        mqttc._handle_pubackcomp("PUBACK")

    assert mqttc._out_messages[message.mid] is message
    assert mqttc._inflight_messages == 1
    assert message.info.is_published() is False
    assert message.state == client.mqtt_ms_wait_for_puback

    mqttc.on_publish = lambda *args: None
    assert mqttc._handle_pubackcomp("PUBACK") == client.MQTT_ERR_SUCCESS
    assert message.info.is_published() is True
    assert mqttc._out_messages == {}
    assert mqttc._inflight_messages == 0


def test_suppressed_on_publish_exception_completes_ack():
    mqttc, message = _outgoing_client()
    mqttc.suppress_exceptions = True

    def fail(*args):
        raise RuntimeError("suppressed callback failure")

    mqttc.on_publish = fail
    assert mqttc._handle_pubackcomp("PUBACK") == client.MQTT_ERR_SUCCESS
    assert message.info.is_published() is True
    assert mqttc._out_messages == {}
    assert mqttc._inflight_messages == 0


def test_callback_exception_after_concurrent_reset_restores_replay_state():
    mqttc, message = _outgoing_client()
    entered = threading.Event()
    release = threading.Event()

    def fail_after_reset(*args):
        entered.set()
        assert release.wait(2.0)
        raise RuntimeError("callback failed after reset")

    mqttc.on_publish = fail_after_reset
    ack_thread, _ack_result, ack_error = _run_in_thread(
        lambda: mqttc._handle_pubackcomp("PUBACK")
    )
    assert entered.wait(1.0)
    mqttc._messages_reconnect_reset_out()
    release.set()
    ack_thread.join(1.0)

    assert not ack_thread.is_alive()
    assert len(ack_error) == 1
    assert str(ack_error[0]) == "callback failed after reset"
    assert mqttc._out_messages[message.mid] is message
    assert message.state == client.mqtt_ms_publish
    assert message.dup is True
    assert message.info.is_published() is False
    assert mqttc._inflight_messages == 0


@pytest.mark.parametrize(
    "protocol,cmd,qos,state",
    [
        (MQTTProtocolVersion.MQTTv311, "PUBCOMP", 2, client.mqtt_ms_wait_for_pubcomp),
        (MQTTProtocolVersion.MQTTv5, "PUBACK", 1, client.mqtt_ms_wait_for_puback),
        (MQTTProtocolVersion.MQTTv5, "PUBCOMP", 2, client.mqtt_ms_wait_for_pubcomp),
    ],
)
def test_ack_callback_decoupling_preserves_protocol_variants(protocol, cmd, qos, state):
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        protocol=protocol,
    )
    message = client.MQTTMessage(mid=13, topic=b"audit/out")
    message.qos = qos
    message.state = state
    mqttc._out_messages[message.mid] = message
    mqttc._inflight_messages = 1
    mqttc._max_inflight_messages = 0
    mqttc._in_packet.remaining_length = 2
    mqttc._in_packet.packet = struct.pack("!H", message.mid)
    seen = []

    def on_publish(mqttc, userdata, mid, reason_code, properties):
        seen.append((mid, reason_code.value, properties.isEmpty(), threading.get_ident()))
        assert message.info.is_published() is False

    mqttc.on_publish = on_publish
    callback_thread = threading.get_ident()
    assert mqttc._handle_pubackcomp(cmd) == client.MQTT_ERR_SUCCESS
    assert seen == [(message.mid, 0, True, callback_thread)]
    assert message.info.is_published() is True
    assert mqttc._out_messages == {}
    assert mqttc._inflight_messages == 0


def test_on_publish_can_remove_itself_without_changing_completion():
    mqttc, message = _outgoing_client()
    callbacks = []

    def on_publish(mqttc, userdata, mid, reason_code, properties):
        callbacks.append(mid)
        mqttc.on_publish = None

    mqttc.on_publish = on_publish
    assert mqttc._handle_pubackcomp("PUBACK") == client.MQTT_ERR_SUCCESS
    assert callbacks == [message.mid]


def test_on_publish_can_publish_without_freeing_reserved_slot_early():
    mqttc, message = _outgoing_client()
    nested = []

    def on_publish(mqttc, userdata, mid, reason_code, properties):
        nested.append(mqttc.publish("audit/nested", b"x", qos=1))
        queued = mqttc._out_messages[nested[0].mid]
        assert queued.state == client.mqtt_ms_queued
        assert mqttc._inflight_messages == 1
        assert message.info.is_published() is False

    mqttc.on_publish = on_publish
    assert mqttc._handle_pubackcomp("PUBACK") == client.MQTT_ERR_SUCCESS
    assert len(nested) == 1
    assert nested[0].rc == client.MQTT_ERR_SUCCESS
    assert message.info.is_published() is True
    assert mqttc._out_messages[nested[0].mid].state == client.mqtt_ms_wait_for_puback
    assert mqttc._inflight_messages == 1


def test_reconnect_reset_does_not_wait_for_on_publish():
    mqttc, message = _outgoing_client()
    entered = threading.Event()
    release = threading.Event()

    def on_publish(*args):
        entered.set()
        assert release.wait(2.0)

    mqttc.on_publish = on_publish
    ack_thread, ack_result, ack_error = _run_in_thread(
        lambda: mqttc._handle_pubackcomp("PUBACK")
    )
    assert entered.wait(1.0)

    reset_thread, _reset_result, reset_error = _run_in_thread(
        mqttc._messages_reconnect_reset_out
    )
    reset_thread.join(0.25)
    assert not reset_thread.is_alive(), "reconnect reset waited for user callback"
    assert reset_error == []
    assert mqttc._inflight_messages == 0
    assert mqttc._out_messages[message.mid] is message

    release.set()
    ack_thread.join(1.0)
    assert not ack_thread.is_alive()
    assert ack_error == []
    assert ack_result == [client.MQTT_ERR_SUCCESS]
    assert mqttc._inflight_messages == 0
    assert mqttc._out_messages == {}
    assert message.info.is_published() is True


def test_on_message_for_pubrel_runs_without_incoming_state_lock():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._manual_ack = True
    message = client.MQTTMessage(mid=11, topic=b"audit/in")
    message.qos = 2
    message.state = client.mqtt_ms_wait_for_pubrel
    mqttc._in_messages[message.mid] = message
    mqttc._in_packet.remaining_length = 2
    mqttc._in_packet.packet = struct.pack("!H", message.mid)
    entered = threading.Event()
    release = threading.Event()

    def on_message(*args):
        entered.set()
        assert release.wait(2.0)

    mqttc.on_message = on_message
    pubrel_thread, pubrel_result, pubrel_error = _run_in_thread(mqttc._handle_pubrel)
    assert entered.wait(1.0)

    reset_thread, _reset_result, reset_error = _run_in_thread(
        mqttc._messages_reconnect_reset_in
    )
    reset_thread.join(0.25)
    assert not reset_thread.is_alive(), "incoming reset waited for user callback"
    assert reset_error == []
    assert mqttc._in_messages == {}

    release.set()
    pubrel_thread.join(1.0)
    assert not pubrel_thread.is_alive()
    assert pubrel_error == []
    assert pubrel_result == [client.MQTT_ERR_SUCCESS]


def test_unsuppressed_pubrel_callback_exception_keeps_historical_pop_semantics():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._manual_ack = True
    message = client.MQTTMessage(mid=17, topic=b"audit/in")
    message.qos = 2
    message.state = client.mqtt_ms_wait_for_pubrel
    mqttc._in_messages[message.mid] = message
    mqttc._in_packet.remaining_length = 2
    mqttc._in_packet.packet = struct.pack("!H", message.mid)

    def fail(*args):
        raise RuntimeError("inbound callback failed")

    mqttc.on_message = fail
    with pytest.raises(RuntimeError, match="inbound callback failed"):
        mqttc._handle_pubrel()
    # The legacy implementation popped before invoking the callback; moving
    # the callback outside the lock must not redeliver after an exception.
    assert mqttc._in_messages == {}
