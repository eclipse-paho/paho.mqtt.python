import socket
import sys
import threading
import time

import pytest

import paho.mqtt.client as client
from paho.mqtt.enums import CallbackAPIVersion, _ConnectionState


class CountingSockpair:
    def __init__(self):
        self.sends = 0
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

    def close(self):
        return None

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


class PartialSendSocket:
    """Accepts at most `chunk` bytes per send to exercise partial writes."""

    def __init__(self, chunk=4):
        self.chunk = chunk
        self.bytes_sent = 0
        self.calls = 0

    def recv(self, size):
        return b""

    def send(self, data):
        self.calls += 1
        n = min(self.chunk, len(data))
        self.bytes_sent += n
        return n

    def close(self):
        return None

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


class RecordingPartialSendSocket(PartialSendSocket):
    def __init__(self, chunk=4):
        super().__init__(chunk)
        self.data = bytearray()

    def send(self, data):
        n = min(self.chunk, len(data))
        self.calls += 1
        self.bytes_sent += n
        self.data.extend(data[:n])
        return n


class FailOnSecondSendSocket(RecordingPartialSendSocket):
    def __init__(self):
        super().__init__(chunk=1 << 30)

    def send(self, data):
        if self.calls == 1:
            self.calls += 1
            raise BlockingIOError()
        return super().send(data)


def test_native_socketpair_is_nonblocking_and_duplex():
    sock1, sock2 = client._socketpair_compat()
    try:
        assert isinstance(sock1, socket.socket)
        assert isinstance(sock2, socket.socket)
        assert sock1.getblocking() is False
        assert sock2.getblocking() is False

        assert sock1.send(b"a") == 1
        assert sock2.recv(1) == b"a"
        assert sock2.send(b"b") == 1
        assert sock1.recv(1) == b"b"
        with pytest.raises(BlockingIOError):
            sock1.recv(1)
    finally:
        sock1.close()
        sock2.close()


def test_loop_start_replaces_and_closes_native_socketpairs(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)

    def wait_until_stopped(*args, **kwargs):
        mqttc._thread_terminate_event.wait(1.0)

    monkeypatch.setattr(mqttc, "loop_forever", wait_until_stopped)
    previous = None
    try:
        for _ in range(20):
            assert mqttc.loop_start() == client.MQTT_ERR_SUCCESS
            current = (mqttc._sockpairR, mqttc._sockpairW)
            assert current[0] is not None and current[1] is not None
            assert current[0].getblocking() is False
            assert current[1].getblocking() is False
            if previous is not None:
                assert previous[0].fileno() == -1
                assert previous[1].fileno() == -1
            assert mqttc.loop_stop() == client.MQTT_ERR_SUCCESS
            previous = current
    finally:
        mqttc._reset_sockets(sockpair_only=True)

    assert previous is not None
    assert previous[0].fileno() == -1
    assert previous[1].fileno() == -1


class FakeSendSocket:
    def __init__(self):
        self.bytes_sent = 0
        self.calls = 0

    def recv(self, size):
        return b""

    def send(self, data):
        self.calls += 1
        self.bytes_sent += len(data)
        return len(data)

    def close(self):
        return None

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


class RecordingSendSocket(FakeSendSocket):
    def __init__(self):
        super().__init__()
        self.packets = []

    def send(self, data):
        self.packets.append(bytes(data))
        return super().send(data)


def test_message_info_condition_is_lazy():
    info = client.MQTTMessageInfo(1)

    assert info._condition is None
    assert info.is_published() is False
    assert info._condition is None

    info._set_as_published()
    assert info._condition is None
    assert info.is_published() is True

    info.wait_for_publish(timeout=0.01)
    assert info._condition is None


def test_wait_for_publish_creates_condition_and_wakes():
    info = client.MQTTMessageInfo(7)
    started = threading.Event()
    done = threading.Event()

    def waiter():
        started.set()
        info.wait_for_publish(timeout=1.0)
        done.set()

    thread = threading.Thread(target=waiter)
    thread.start()
    assert started.wait(1.0)
    for _ in range(50):
        if info._condition is not None:
            break
        time.sleep(0.01)

    assert info._condition is not None
    info._set_as_published()
    assert done.wait(1.0)
    thread.join(1.0)


def test_wait_for_publish_does_not_miss_concurrent_set_as_published():
    """Race: waiter and publisher must not lose the published notification."""
    failures = []

    for _ in range(200):
        info = client.MQTTMessageInfo(1)
        barrier = threading.Barrier(2)
        done = threading.Event()

        def waiter():
            barrier.wait()
            info.wait_for_publish(timeout=1.0)
            if not info.is_published():
                failures.append("waiter returned unpublished")
            done.set()

        def publisher():
            barrier.wait()
            info._set_as_published()

        threads = [
            threading.Thread(target=waiter),
            threading.Thread(target=publisher),
        ]
        for thread in threads:
            thread.start()
        assert done.wait(2.0), "wait_for_publish timed out under race"
        for thread in threads:
            thread.join(1.0)

    assert failures == []


def test_packet_queue_coalesces_sockpair_wakeups():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    sockpair = CountingSockpair()
    mqttc._sockpairW = sockpair
    mqttc._sockpairR = sockpair
    mqttc._thread = threading.Thread(target=lambda: None)
    mqttc._thread_terminate = True

    for mid in range(100):
        rc = mqttc._packet_queue(client.PUBLISH, b"x", mid, 0)
        assert rc == client.MQTT_ERR_SUCCESS

    assert sockpair.sends == 1
    assert mqttc._sockpair_wakeup_pending is True

    with mqttc._sockpair_wakeup_mutex:
        sockpair.recv(10000)
        mqttc._sockpair_wakeup_pending = False

    rc = mqttc._packet_queue(client.PUBLISH, b"y", 101, 0)
    assert rc == client.MQTT_ERR_SUCCESS
    assert sockpair.sends == 2


def test_partial_socket_writes_preserve_packet_and_publish_state():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = PartialSendSocket(chunk=3)
    mqttc._state = _ConnectionState.MQTT_CS_CONNECTED
    # Force queue path so publish() does not drain immediately.
    mqttc._thread = threading.Thread(target=lambda: None)

    info = mqttc.publish("a/b", b"0123456789", qos=0)
    assert info.rc == client.MQTT_ERR_SUCCESS
    assert info.is_published() is False
    assert mqttc.want_write() is True

    # Drain across many partial writes.
    for _ in range(100):
        rc = mqttc.loop_write()
        assert rc in (client.MQTT_ERR_SUCCESS, client.MQTT_ERR_AGAIN)
        if info.is_published():
            break

    assert info.is_published() is True
    assert mqttc.want_write() is False
    assert mqttc._sock.bytes_sent > 0


def test_large_immutable_publish_queues_header_and_payload_segments():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._thread = threading.Thread(target=lambda: None)
    payload = b"x" * (1024 * 1024)

    info = mqttc.publish("large/topic", payload, qos=0)

    assert info.rc == client.MQTT_ERR_SUCCESS
    assert len(mqttc._out_packet) == 1
    packet = mqttc._out_packet[0]
    assert isinstance(packet["packet"], tuple)
    assert packet["packet"][1] is payload
    assert packet["to_process"] == sum(len(segment) for segment in packet["packet"])


def test_segmented_publish_partial_writes_preserve_exact_wire_bytes():
    payload = b"0123456789abcdef" * (64 * 1024)
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = RecordingPartialSendSocket(chunk=4096)
    mqttc._thread = threading.Thread(target=lambda: None)

    info = mqttc.publish("large/topic", payload, qos=0)
    header = mqttc._out_packet[0]["packet"][0]

    assert mqttc.loop_write() == client.MQTT_ERR_SUCCESS
    assert bytes(mqttc._sock.data) == header + payload
    assert info.is_published()
    assert not mqttc._out_packet


def test_mutable_and_websocket_payloads_keep_contiguous_snapshot():
    mutable = bytearray(b"x" * (1024 * 1024))
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._thread = threading.Thread(target=lambda: None)

    mqttc.publish("large/topic", mutable, qos=0)
    packet = mqttc._out_packet[0]["packet"]
    assert isinstance(packet, bytearray)
    mutable[:] = b"y" * len(mutable)
    assert packet.endswith(b"x" * (1024 * 1024))

    websocket_client = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        transport="websockets",
    )
    websocket_client._sock = FakeSendSocket()
    websocket_client._thread = threading.Thread(target=lambda: None)
    websocket_client.publish("large/topic", b"z" * (1024 * 1024), qos=0)
    assert isinstance(websocket_client._out_packet[0]["packet"], bytearray)


def test_small_immutable_publish_keeps_contiguous_packet():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._thread = threading.Thread(target=lambda: None)

    mqttc.publish("small/topic", b"x" * 1024, qos=0)

    assert isinstance(mqttc._out_packet[0]["packet"], bytearray)


def test_segmented_payload_reference_is_released_after_completion():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._thread = threading.Thread(target=lambda: None)
    payload = b"x" * (1024 * 1024)
    initial_references = sys.getrefcount(payload)

    mqttc.publish("large/topic", payload, qos=0)
    assert sys.getrefcount(payload) == initial_references + 1

    assert mqttc.loop_write() == client.MQTT_ERR_SUCCESS
    assert sys.getrefcount(payload) == initial_references


def test_segmented_payload_eagain_after_header_resumes_at_payload():
    payload = b"x" * (1024 * 1024)
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    first_socket = FailOnSecondSendSocket()
    mqttc._sock = first_socket
    mqttc._thread = threading.Thread(target=lambda: None)

    mqttc.publish("large/topic", payload, qos=0)
    header = mqttc._out_packet[0]["packet"][0]

    assert mqttc.loop_write() == client.MQTT_ERR_SUCCESS
    assert first_socket.data == header
    assert mqttc._out_packet[0]["pos"] == len(header)

    second_socket = RecordingPartialSendSocket(chunk=4096)
    mqttc._sock = second_socket
    assert mqttc.loop_write() == client.MQTT_ERR_SUCCESS
    assert second_socket.data == payload


def test_qos0_on_publish_ordering():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._state = _ConnectionState.MQTT_CS_CONNECTED
    seen = []

    def on_publish(mqttc, userdata, mid, reason_code, properties):
        seen.append(mid)

    mqttc.on_publish = on_publish

    infos = [mqttc.publish("t/{}".format(i), b"x", qos=0) for i in range(5)]
    assert [info.mid for info in infos] == seen
    assert all(info.is_published() for info in infos)


def test_external_loop_register_write_on_packet_queue():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    registered = []

    def on_register(mqttc, userdata, sock):
        registered.append(sock)

    mqttc.on_socket_register_write = on_register
    # External-loop mode: no network thread, register callback set, so
    # _packet_queue must not call loop_write() immediately.
    mqttc._thread = None
    mqttc._sock = FakeSendSocket()
    mqttc._registered_write = False

    rc = mqttc._packet_queue(client.PUBLISH, b"payload", 1, 0)
    assert rc == client.MQTT_ERR_SUCCESS
    assert registered == [mqttc._sock]
    assert mqttc.want_write() is True
    assert len(mqttc._out_packet) == 1


def test_external_loop_unregister_write_after_drain():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    registered = []
    unregistered = []

    def on_register(mqttc, userdata, sock):
        registered.append(sock)

    def on_unregister(mqttc, userdata, sock):
        unregistered.append(sock)

    mqttc.on_socket_register_write = on_register
    mqttc.on_socket_unregister_write = on_unregister
    mqttc._thread = None
    mqttc._sock = FakeSendSocket()
    mqttc._state = _ConnectionState.MQTT_CS_CONNECTED
    mqttc._registered_write = False

    rc = mqttc._packet_queue(client.PUBLISH, b"payload", 1, 0)
    assert rc == client.MQTT_ERR_SUCCESS
    assert registered == [mqttc._sock]
    assert mqttc.want_write() is True

    assert mqttc.loop_write() == client.MQTT_ERR_SUCCESS
    assert unregistered == [mqttc._sock]
    assert mqttc.want_write() is False
    assert mqttc._registered_write is False


def test_publish_from_on_message_defers_loop_write():
    """Publish inside on_message must queue without re-entering loop_write."""
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._state = _ConnectionState.MQTT_CS_CONNECTED
    mqttc._thread = None
    loop_write_calls = []
    real_loop_write = mqttc.loop_write

    def counting_loop_write():
        loop_write_calls.append(1)
        return real_loop_write()

    mqttc.loop_write = counting_loop_write

    in_callback_during_publish = []

    def on_message(mqttc, userdata, msg):
        in_callback_during_publish.append(mqttc._in_callback_mutex.locked())
        info = mqttc.publish("reply/topic", b"reply", qos=0)
        assert info.rc == client.MQTT_ERR_SUCCESS
        in_callback_during_publish.append(mqttc._in_callback_mutex.locked())

    mqttc.on_message = on_message

    message = client.MQTTMessage(create_info=False)
    message.topic = "request/topic"
    message.payload = b"ping"
    message.qos = 0

    mqttc._handle_on_message(message)

    assert in_callback_during_publish == [True, True]
    assert mqttc.want_write() is True
    assert loop_write_calls == []

    assert mqttc.loop_write() == client.MQTT_ERR_SUCCESS
    assert mqttc.want_write() is False
    assert loop_write_calls == [1]


def test_publish_from_on_message_threaded_coalesces_wakeup():
    """Threaded mode: follow-up publish from on_message uses one sockpair wakeup."""
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._state = _ConnectionState.MQTT_CS_CONNECTED
    sockpair = CountingSockpair()
    mqttc._sockpairW = sockpair
    mqttc._sockpairR = sockpair
    mqttc._thread = threading.Thread(target=lambda: None)
    mqttc._thread_terminate = True

    def on_message(mqttc, userdata, msg):
        mqttc.publish("reply/topic", b"reply", qos=0)

    mqttc.on_message = on_message

    message = client.MQTTMessage(create_info=False)
    message.topic = "request/topic"
    message.payload = b"ping"
    message.qos = 0

    mqttc._handle_on_message(message)

    assert sockpair.sends == 1
    assert mqttc._sockpair_wakeup_pending is True
    assert mqttc.want_write() is True


def test_loop_start_clears_stale_sockpair_wakeup_pending(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sockpair_wakeup_pending = True
    sock_r = CountingSockpair()
    sock_w = CountingSockpair()

    monkeypatch.setattr(client, "_socketpair_compat", lambda: (sock_r, sock_w))

    def fake_thread_main():
        return None

    monkeypatch.setattr(mqttc, "_thread_main", fake_thread_main)

    assert mqttc.loop_start() == client.MQTT_ERR_SUCCESS
    assert mqttc._sockpair_wakeup_pending is False
    assert mqttc._sockpairR is sock_r
    assert mqttc._sockpairW is sock_w
    mqttc.loop_stop()


def test_loop_start_does_not_lose_wakeup_to_concurrent_packet_queue(monkeypatch):
    """Publish during sockpair swap must not leave pending=True on an empty pair."""
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    old_r = CountingSockpair()
    old_w = CountingSockpair()
    new_r = CountingSockpair()
    new_w = CountingSockpair()
    mqttc._sockpairR = old_r
    mqttc._sockpairW = old_w
    # Avoid immediate loop_write() while _thread is still None during loop_start.
    mqttc.on_socket_register_write = lambda *args: None

    release = threading.Event()

    def slow_socketpair():
        release.wait(1.0)
        return new_r, new_w

    monkeypatch.setattr(client, "_socketpair_compat", slow_socketpair)
    monkeypatch.setattr(mqttc, "_thread_main", lambda: None)

    starter = threading.Thread(target=mqttc.loop_start)
    starter.start()
    time.sleep(0.01)
    for mid in range(20):
        mqttc._packet_queue(client.PUBLISH, b"x", mid, 0)
    release.set()
    starter.join(2.0)

    assert mqttc._sockpairR is new_r
    assert mqttc._sockpairW is new_w
    assert len(mqttc._out_packet) == 20
    assert new_w.sends == 1
    assert mqttc._sockpair_wakeup_pending is True
    mqttc.loop_stop()


def test_loop_stop_sets_event_and_coalesces_selector_wakeup():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    sockpair = CountingSockpair()
    mqttc._sockpairR = sockpair
    mqttc._sockpairW = sockpair
    stopped = threading.Event()

    def wait_for_stop():
        mqttc._thread_terminate_event.wait(1.0)
        stopped.set()

    mqttc._thread = threading.Thread(target=wait_for_stop)
    mqttc._thread.start()

    started = time.perf_counter()
    assert mqttc.loop_stop() == client.MQTT_ERR_SUCCESS

    assert time.perf_counter() - started < 0.05
    assert stopped.is_set()
    assert sockpair.sends == 1
    assert mqttc._sockpair_wakeup_pending is True


def test_loop_stop_joins_captured_thread_when_worker_clears_client_reference(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    release = threading.Event()

    def clear_thread_reference():
        release.wait(1.0)
        mqttc._thread = None

    thread = threading.Thread(target=clear_thread_reference)
    mqttc._thread = thread
    thread.start()

    def wake_and_allow_worker_exit():
        release.set()
        thread.join(1.0)

    monkeypatch.setattr(mqttc, "_wake_thread", wake_and_allow_worker_exit)

    assert mqttc.loop_stop() == client.MQTT_ERR_SUCCESS
    assert not thread.is_alive()


def test_reconnect_wait_is_interrupted_by_thread_stop():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._reconnect_min_delay = 60
    mqttc._reconnect_max_delay = 60
    waiter = threading.Thread(target=mqttc._reconnect_wait)
    waiter.start()
    time.sleep(0.01)

    started = time.perf_counter()
    mqttc._thread_terminate = True
    mqttc._thread_terminate_event.set()
    waiter.join(0.05)

    assert not waiter.is_alive()
    assert time.perf_counter() - started < 0.05


def test_internal_thread_uses_keepalive_as_interruptible_loop_deadline(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._keepalive = 300
    call = {}

    def fake_loop_forever(timeout=1.0, retry_first_connection=False):
        call["timeout"] = timeout
        call["retry_first_connection"] = retry_first_connection

    monkeypatch.setattr(mqttc, "loop_forever", fake_loop_forever)

    mqttc._thread_main()

    assert call == {"timeout": 300.0, "retry_first_connection": True}


def test_thread_loop_timeout_preserves_active_tick_and_idle_deadline(monkeypatch):
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._keepalive = 60
    mqttc._last_msg_in = 100.0
    mqttc._last_msg_out = 100.0

    monkeypatch.setattr(client, "time_func", lambda: 100.5)
    assert mqttc._thread_loop_timeout(60.0) == 1.0

    monkeypatch.setattr(client, "time_func", lambda: 110.0)
    assert mqttc._thread_loop_timeout(60.0) == 50.0

    monkeypatch.setattr(client, "time_func", lambda: 160.0)
    assert mqttc._thread_loop_timeout(60.0) == 0.0


def test_pack_remaining_length_fast_path_and_size_limit():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)

    small = bytearray()
    mqttc._pack_remaining_length(small, 127)
    assert bytes(small) == b"\x7f"

    multi = bytearray()
    mqttc._pack_remaining_length(multi, 128)
    assert bytes(multi) == b"\x80\x01"

    max_ok = bytearray()
    mqttc._pack_remaining_length(max_ok, 268_435_455)
    assert bytes(max_ok) == b"\xff\xff\xff\x7f"

    with pytest.raises(ValueError, match="Packet too large"):
        mqttc._pack_remaining_length(bytearray(), 268_435_456)


def test_reconnect_reset_computes_clean_session_once(monkeypatch):
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="reconnect-test",
        clean_session=False,
    )
    calls = []

    def check_clean_session():
        calls.append(1)
        return False

    monkeypatch.setattr(mqttc, "_check_clean_session", check_clean_session)
    for mid in range(1, 101):
        message = client.MQTTMessage(mid=mid, topic=b"devices/topic")
        message.qos = 2
        message.state = client.mqtt_ms_wait_for_pubrec
        mqttc._out_messages[mid] = message

    mqttc._messages_reconnect_reset_out()

    assert calls == [1]
    assert [message.state for message in mqttc._out_messages.values()] == [client.mqtt_ms_publish] * 100
    assert all(message.dup for message in mqttc._out_messages.values())


def test_message_state_dicts_preserve_insertion_order_and_clean_reset():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)

    assert type(mqttc._out_messages) is dict
    assert type(mqttc._in_messages) is dict

    for mid in (3, 1, 2):
        message = client.MQTTMessage(mid=mid, topic=b"devices/topic")
        mqttc._out_messages[mid] = message
        mqttc._in_messages[mid] = message

    reinserted = mqttc._out_messages.pop(1)
    mqttc._out_messages[1] = reinserted
    assert list(mqttc._out_messages) == [3, 2, 1]

    mqttc._messages_reconnect_reset_in()
    assert type(mqttc._in_messages) is dict
    assert mqttc._in_messages == {}


def test_update_inflight_reuses_internal_topic_bytes():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    mqttc._sock = FakeSendSocket()
    mqttc._state = _ConnectionState.MQTT_CS_CONNECTED
    mqttc._max_inflight_messages = 1
    message = client.MQTTMessage(mid=1, topic=b"devices/topic")
    message._topic_str = object()
    message.payload = b"payload"
    message.qos = 1
    message.state = client.mqtt_ms_queued
    mqttc._out_messages[1] = message

    assert mqttc._update_inflight() == client.MQTT_ERR_SUCCESS
    assert message.state == client.mqtt_ms_wait_for_puback
    assert mqttc._inflight_messages == 1


def test_connack_replay_staging_is_bounded_and_preserves_order_and_dup():
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="replay-test",
        clean_session=False,
    )
    mqttc._sock = RecordingSendSocket()
    mqttc._max_inflight_messages = 130
    topic = b"devices/topic"
    for mid in range(1, 131):
        message = client.MQTTMessage(mid=mid, topic=topic)
        message.payload = b"payload"
        message.qos = 1
        message.state = client.mqtt_ms_publish
        message.dup = True
        mqttc._out_messages[mid] = message

    mqttc._in_packet.packet = bytearray((0, 0))
    mqttc._in_packet.remaining_length = 2
    real_loop_write = mqttc.loop_write
    queue_depths = []

    def counting_loop_write():
        queue_depths.append(len(mqttc._out_packet))
        return real_loop_write()

    mqttc.loop_write = counting_loop_write

    assert mqttc._handle_connack() == client.MQTT_ERR_SUCCESS
    assert queue_depths == [64, 64, 2]
    assert mqttc._inflight_messages == 130
    assert all(message.state == client.mqtt_ms_wait_for_puback for message in mqttc._out_messages.values())
    assert all(packet[0] & 0x08 for packet in mqttc._sock.packets)

    mids = []
    for packet in mqttc._sock.packets:
        topic_length = int.from_bytes(packet[2:4], "big")
        mid_pos = 4 + topic_length
        mids.append(int.from_bytes(packet[mid_pos:mid_pos + 2], "big"))
    assert mids == list(range(1, 131))


def test_connack_replay_staging_is_bounded_by_payload_bytes():
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="replay-large-test",
        clean_session=False,
    )
    mqttc._sock = FakeSendSocket()
    mqttc._max_inflight_messages = 3
    for mid in range(1, 4):
        message = client.MQTTMessage(mid=mid, topic=b"t")
        message.payload = b"x" * 40000
        message.qos = 1
        message.state = client.mqtt_ms_publish
        mqttc._out_messages[mid] = message

    mqttc._in_packet.packet = bytearray((0, 0))
    mqttc._in_packet.remaining_length = 2
    real_loop_write = mqttc.loop_write
    queue_depths = []

    def counting_loop_write():
        queue_depths.append(len(mqttc._out_packet))
        return real_loop_write()

    mqttc.loop_write = counting_loop_write

    assert mqttc._handle_connack() == client.MQTT_ERR_SUCCESS
    assert queue_depths == [2, 1]


def test_connack_replay_preserves_qos2_publish_and_pubrel_states():
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="replay-qos2-test",
        clean_session=False,
    )
    mqttc._sock = RecordingSendSocket()
    mqttc._max_inflight_messages = 2

    publish = client.MQTTMessage(mid=1, topic=b"t")
    publish.payload = b"payload"
    publish.qos = 2
    publish.state = client.mqtt_ms_publish
    publish.dup = True
    mqttc._out_messages[1] = publish

    pubrel = client.MQTTMessage(mid=2, topic=b"t")
    pubrel.qos = 2
    pubrel.state = client.mqtt_ms_resend_pubrel
    mqttc._out_messages[2] = pubrel

    mqttc._in_packet.packet = bytearray((0, 0))
    mqttc._in_packet.remaining_length = 2

    assert mqttc._handle_connack() == client.MQTT_ERR_SUCCESS
    assert publish.state == client.mqtt_ms_wait_for_pubrec
    assert pubrel.state == client.mqtt_ms_wait_for_pubcomp
    assert mqttc._inflight_messages == 2
    assert [packet[0] & 0xF0 for packet in mqttc._sock.packets] == [client.PUBLISH, client.PUBREL]
    assert mqttc._sock.packets[0][0] & 0x08


def test_connack_replay_pack_failure_flushes_prior_packets_and_stops():
    mqttc = client.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="replay-failure-test",
        clean_session=False,
    )
    mqttc._sock = FakeSendSocket()
    mqttc._max_inflight_messages = 4
    for mid in range(1, 5):
        message = client.MQTTMessage(mid=mid, topic=b"t")
        message.payload = b"payload"
        message.qos = 1
        message.state = client.mqtt_ms_publish
        mqttc._out_messages[mid] = message

    mqttc._in_packet.packet = bytearray((0, 0))
    mqttc._in_packet.remaining_length = 2
    real_send_publish = mqttc._send_publish
    calls = []

    def failing_send_publish(mid, *args, **kwargs):
        calls.append(mid)
        if mid == 3:
            return client.MQTT_ERR_NOMEM
        return real_send_publish(mid, *args, **kwargs)

    mqttc._send_publish = failing_send_publish

    assert mqttc._handle_connack() == client.MQTT_ERR_NOMEM
    assert calls == [1, 2, 3]
    assert len(mqttc._out_packet) == 0
    assert mqttc._sock.calls == 2
    assert mqttc._out_messages[4].state == client.mqtt_ms_publish
