import collections

import pytest

from paho import mqtt
from paho.mqtt import client, publish


class _Info:
    def __init__(self, mid, rc=client.MQTT_ERR_SUCCESS):
        self.mid = mid
        self.rc = rc


def _messages(count, mixed=False):
    result = []
    for index in range(count):
        qos = index % 3 if mixed else 1
        result.append({
            "topic": "audit/{:04d}".format(index),
            "payload": b"x",
            "qos": qos,
        })
    return result


def _install_fake_client(
    monkeypatch,
    *,
    fail_at=None,
    no_conn_at=None,
    raise_at=None,
    synchronous=False,
    duplicate_callbacks=False,
    reconnect_before_ack=False,
    connack_reason=0,
    mids=None,
):
    instances = []

    class FakeClient:
        def __init__(self, callback_api_version, client_id, userdata, protocol, transport):
            self._userdata = userdata
            self.on_connect = None
            self.on_publish = None
            self.events = collections.deque()
            self.publish_order = []
            self.publish_calls = 0
            self.callback_count = 0
            self.wire_outstanding = 0
            self.peak_outstanding = 0
            self.disconnect_count = 0
            self.disconnect_outstanding = []
            self.disconnected = False
            instances.append(self)

        def enable_logger(self):
            return None

        def connect(self, hostname, port, keepalive):
            return client.MQTT_ERR_SUCCESS

        def publish(self, topic, payload=None, qos=0, retain=False):
            self.publish_calls += 1
            if raise_at == self.publish_calls:
                raise ValueError("publish exploded")
            if fail_at == self.publish_calls:
                return _Info(self.publish_calls, client.MQTT_ERR_QUEUE_SIZE)

            if mids is None:
                mid = self.publish_calls
            else:
                mid = mids[self.publish_calls - 1]
            self.publish_order.append(topic)
            self.wire_outstanding += 1
            self.peak_outstanding = max(
                self.peak_outstanding, self.wire_outstanding
            )
            if synchronous:
                self.wire_outstanding -= 1
                self.callback_count += 1
                self.on_publish(self, self._userdata, mid, None, None)
            else:
                self.events.append(mid)
            rc = (
                client.MQTT_ERR_NO_CONN
                if no_conn_at == self.publish_calls
                else client.MQTT_ERR_SUCCESS
            )
            if rc != client.MQTT_ERR_SUCCESS and qos == 0:
                self.events.pop()
                self.wire_outstanding -= 1
            return _Info(mid, rc)

        def disconnect(self):
            self.disconnect_count += 1
            self.disconnect_outstanding.append(self.wire_outstanding)
            self.disconnected = True
            return client.MQTT_ERR_SUCCESS

        def loop_forever(self):
            self.on_connect(
                self, self._userdata, {}, connack_reason, None
            )
            if reconnect_before_ack and not self.disconnected:
                self.on_connect(self, self._userdata, {}, 0, None)
            while self.events and not self.disconnected:
                mid = self.events.popleft()
                self.wire_outstanding -= 1
                self.callback_count += 1
                self.on_publish(self, self._userdata, mid, None, None)
                if duplicate_callbacks:
                    self.on_publish(self, self._userdata, mid, None, None)
            if not self.disconnected:
                raise RuntimeError("helper did not disconnect")
            if self.events or self.wire_outstanding:
                raise RuntimeError("helper disconnected before completion")

    monkeypatch.setattr(publish.paho, "Client", FakeClient)
    return instances


def test_multiple_keeps_twenty_messages_outstanding_and_preserves_order(monkeypatch):
    instances = _install_fake_client(monkeypatch)
    messages = _messages(100)

    publish.multiple(messages)

    mqttc = instances[0]
    assert mqttc.publish_order == [message["topic"] for message in messages]
    assert mqttc.peak_outstanding == 20
    assert mqttc.callback_count == 100
    assert mqttc.disconnect_count == 1
    assert mqttc.disconnect_outstanding == [0]


def test_multiple_mixed_qos_uses_same_bounded_window(monkeypatch):
    instances = _install_fake_client(monkeypatch)
    messages = _messages(100, mixed=True)

    publish.multiple(messages)

    mqttc = instances[0]
    assert mqttc.publish_order == [message["topic"] for message in messages]
    assert mqttc.peak_outstanding == 20
    assert mqttc.callback_count == 100
    assert mqttc.disconnect_outstanding == [0]


@pytest.mark.parametrize("count,expected_peak", [(1, 1), (20, 20), (21, 20)])
def test_multiple_disconnects_only_after_exact_window_completion(
    monkeypatch, count, expected_peak
):
    instances = _install_fake_client(monkeypatch)

    publish.multiple(_messages(count))

    mqttc = instances[0]
    assert mqttc.peak_outstanding == expected_peak
    assert mqttc.callback_count == count
    assert mqttc.disconnect_count == 1
    assert mqttc.disconnect_outstanding == [0]


def test_multiple_drains_accepted_messages_before_raising_queue_error(monkeypatch):
    instances = _install_fake_client(monkeypatch, fail_at=6)

    with pytest.raises(mqtt.MQTTException, match="Message queue full"):
        publish.multiple(_messages(30))

    mqttc = instances[0]
    assert mqttc.publish_calls == 6
    assert mqttc.callback_count == 5
    assert mqttc.disconnect_count == 1
    assert mqttc.disconnect_outstanding == [0]


def test_multiple_drains_accepted_messages_before_reraising_publish_error(monkeypatch):
    instances = _install_fake_client(monkeypatch, raise_at=6)

    with pytest.raises(ValueError, match="publish exploded"):
        publish.multiple(_messages(30))

    mqttc = instances[0]
    assert mqttc.publish_calls == 6
    assert mqttc.callback_count == 5
    assert mqttc.disconnect_outstanding == [0]


def test_multiple_keeps_qos1_message_queued_during_transient_disconnect(monkeypatch):
    instances = _install_fake_client(monkeypatch, no_conn_at=6)

    publish.multiple(_messages(30))

    mqttc = instances[0]
    assert mqttc.publish_calls == 30
    assert mqttc.callback_count == 30
    assert mqttc.disconnect_outstanding == [0]


def test_multiple_rejects_qos0_message_not_queued_during_disconnect(monkeypatch):
    instances = _install_fake_client(monkeypatch, no_conn_at=6)
    messages = _messages(30)
    messages[5]["qos"] = 0

    with pytest.raises(mqtt.MQTTException, match="not currently connected"):
        publish.multiple(messages)

    mqttc = instances[0]
    assert mqttc.publish_calls == 6
    assert mqttc.callback_count == 5
    assert mqttc.disconnect_outstanding == [0]


def test_multiple_handles_synchronous_publish_callback_reentrancy(monkeypatch):
    instances = _install_fake_client(monkeypatch, synchronous=True)

    publish.multiple(_messages(100))

    mqttc = instances[0]
    assert mqttc.publish_calls == 100
    assert mqttc.callback_count == 100
    assert mqttc.peak_outstanding == 1
    assert mqttc.disconnect_count == 1


def test_multiple_ignores_duplicate_completion_callbacks(monkeypatch):
    instances = _install_fake_client(monkeypatch, duplicate_callbacks=True)

    publish.multiple(_messages(100))

    mqttc = instances[0]
    assert mqttc.publish_calls == 100
    assert mqttc.callback_count == 100
    assert mqttc.disconnect_count == 1


def test_multiple_reconnect_does_not_exceed_window(monkeypatch):
    instances = _install_fake_client(monkeypatch, reconnect_before_ack=True)

    publish.multiple(_messages(100))

    mqttc = instances[0]
    assert mqttc.publish_calls == 100
    assert mqttc.peak_outstanding == 20
    assert mqttc.disconnect_count == 1


def test_multiple_handles_mid_wrap_without_collision(monkeypatch):
    mids = list(range(65530, 65536)) + list(range(1, 35))
    instances = _install_fake_client(monkeypatch, mids=mids)

    publish.multiple(_messages(40))

    mqttc = instances[0]
    assert mqttc.publish_calls == 40
    assert mqttc.callback_count == 40
    assert mqttc.peak_outstanding == 20


def test_multiple_connack_failure_disconnects_and_raises(monkeypatch):
    instances = _install_fake_client(monkeypatch, connack_reason=5)

    with pytest.raises(mqtt.MQTTException):
        publish.multiple(_messages(3))

    mqttc = instances[0]
    assert mqttc.publish_calls == 0
    assert mqttc.disconnect_count == 1
    assert mqttc.disconnect_outstanding == [0]
