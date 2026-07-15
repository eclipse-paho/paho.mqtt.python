import socket
from unittest.mock import Mock

import paho.mqtt.client as client
import pytest
from paho.mqtt.client import WebsocketConnectionError, _WebsocketWrapper


class _PartialWebSocket:
    def __init__(self, incoming=b"", chunk=3, recv_chunk=None, eof=False):
        self.incoming = bytearray(incoming)
        self.chunk = chunk
        self.recv_chunk = recv_chunk
        self.recv_calls = 0
        self.eof = eof
        self.sent = bytearray()

    def recv(self, size):
        self.recv_calls += 1
        if not self.incoming:
            if self.eof:
                return b""
            raise BlockingIOError()
        count = min(size, len(self.incoming))
        if self.recv_chunk is not None:
            count = min(count, self.recv_chunk)
        data = bytes(self.incoming[:count])
        del self.incoming[:count]
        return data

    def send(self, data):
        count = min(self.chunk, len(data))
        self.sent.extend(bytes(data[:count]))
        return count

    def close(self):
        return None


def _wrapper_for_io(sock):
    wrapper = _WebsocketWrapper.__new__(_WebsocketWrapper)
    wrapper.connected = True
    wrapper._ssl = False
    wrapper._socket = sock
    wrapper._sendbuffer = bytearray()
    wrapper._sendbuffer_head = 0
    wrapper._readbuffer = bytearray()
    wrapper._requested_size = 0
    wrapper._readbuffer_head = 0
    reset_inbound = getattr(wrapper, "_reset_inbound", None)
    if reset_inbound is not None:
        reset_inbound()
    return wrapper


def _unmask_client_frame(frame):
    length_code = frame[1] & 0x7F
    pos = 2
    if length_code == 126:
        length = int.from_bytes(frame[pos:pos + 2], "big")
        pos += 2
    elif length_code == 127:
        length = int.from_bytes(frame[pos:pos + 8], "big")
        pos += 8
    else:
        length = length_code
    mask = frame[pos:pos + 4]
    pos += 4
    payload = frame[pos:pos + length]
    return bytes(value ^ mask[index % 4] for index, value in enumerate(payload))


@pytest.mark.parametrize("size", [0, 2, 16, 128, 1024, 65536])
def test_create_frame_masks_payload(monkeypatch, size):
    wrapper = _wrapper_for_io(_PartialWebSocket())
    payload = bytearray(index % 251 for index in range(size))
    original = bytes(payload)
    monkeypatch.setattr(client.os, "urandom", lambda count: b"\x01\x02\x03\x04")

    frame = wrapper._create_frame(_WebsocketWrapper.OPCODE_BINARY, payload)

    assert frame[0] == 0x80 | _WebsocketWrapper.OPCODE_BINARY
    assert frame[1] & 0x80
    assert _unmask_client_frame(frame) == original


def test_partial_websocket_send_uses_cursor_and_emits_one_frame(monkeypatch):
    sock = _PartialWebSocket(chunk=5)
    wrapper = _wrapper_for_io(sock)
    payload = b"partial websocket payload"
    monkeypatch.setattr(client.os, "urandom", lambda count: b"\x05\x06\x07\x08")

    results = []
    for _ in range(100):
        results.append(wrapper.send(payload))
        if results[-1] == len(payload):
            break

    assert results[-1] == len(payload)
    assert all(result == 0 for result in results[:-1])
    assert _unmask_client_frame(sock.sent) == payload
    assert wrapper._sendbuffer == bytearray()
    assert wrapper._sendbuffer_head == 0


def test_websocket_ping_sends_unmasked_pong():
    sock = _PartialWebSocket(incoming=b"\x89\x02hi", chunk=100)
    wrapper = _wrapper_for_io(sock)

    with pytest.raises(BlockingIOError):
        wrapper.recv(10)

    assert bytes(sock.sent) == b"\x8a\x02hi"


@pytest.mark.parametrize("opcode", [_WebsocketWrapper.OPCODE_BINARY, _WebsocketWrapper.OPCODE_CONTINUATION])
def test_websocket_binary_and_continuation_payload(opcode):
    sock = _PartialWebSocket(incoming=bytes([0x80 | opcode, 3]) + b"abc", chunk=100)
    wrapper = _wrapper_for_io(sock)
    if opcode == _WebsocketWrapper.OPCODE_CONTINUATION:
        wrapper._fragmented = True

    assert wrapper.recv(10) == b"abc"


def test_websocket_read_ahead_decodes_multiple_frames_in_one_raw_read():
    stream = b"\x82\x03abc\x82\x03def"
    sock = _PartialWebSocket(incoming=stream, chunk=100)
    wrapper = _wrapper_for_io(sock)

    assert wrapper.recv(65536) == b"abcdef"
    assert sock.recv_calls == 1
    assert wrapper.pending() == 0


def test_websocket_small_exact_reads_do_not_prefetch_the_next_frame():
    stream = b"\x82\x03abc\x82\x03def"
    sock = _PartialWebSocket(incoming=stream, chunk=100)
    wrapper = _wrapper_for_io(sock)

    assert wrapper.recv(1) == b"a"
    assert bytes(sock.incoming) == b"bc\x82\x03def"
    assert wrapper.recv(2) == b"bc"
    assert bytes(sock.incoming) == b"\x82\x03def"
    assert wrapper.recv(3) == b"def"


def test_websocket_fragmentation_allows_interleaved_ping():
    stream = b"\x02\x02ab\x89\x01x\x80\x02cd"
    sock = _PartialWebSocket(incoming=stream, chunk=100)
    wrapper = _wrapper_for_io(sock)

    assert wrapper.recv(65536) == b"abcd"
    assert bytes(sock.sent) == b"\x8a\x01x"


def test_websocket_streaming_handles_one_byte_network_chunks():
    stream = b"\x82\x7e\x00\x80" + b"x" * 128
    sock = _PartialWebSocket(incoming=stream, chunk=100, recv_chunk=1)
    wrapper = _wrapper_for_io(sock)

    assert wrapper.recv(65536) == b"x" * 128
    assert sock.recv_calls == len(stream)


def test_websocket_delivers_buffered_payload_before_eof():
    sock = _PartialWebSocket(incoming=b"\x82\x03abc", chunk=100, eof=True)
    wrapper = _wrapper_for_io(sock)

    assert wrapper.recv(65536) == b"abc"
    assert wrapper.recv(65536) == b""
    assert wrapper.connected is False


def test_websocket_partial_pong_is_exposed_as_pending_write():
    sock = _PartialWebSocket(incoming=b"\x89\x02hi", chunk=2)
    wrapper = _wrapper_for_io(sock)

    with pytest.raises(BlockingIOError):
        wrapper.recv(65536)

    assert wrapper.pending_write() is True
    while wrapper.pending_write():
        wrapper.flush()
    assert bytes(sock.sent) == b"\x8a\x02hi"


def test_websocket_client_loop_flushes_partial_pong():
    sock = _PartialWebSocket(incoming=b"\x89\x02hi", chunk=2)
    wrapper = _wrapper_for_io(sock)
    mqttc = client.Client(client.CallbackAPIVersion.VERSION2, transport="websockets")
    mqttc._sock = wrapper

    with pytest.raises(BlockingIOError):
        wrapper.recv(65536)
    assert mqttc.want_write() is True

    while mqttc.want_write():
        assert mqttc.loop_write() == client.MQTT_ERR_SUCCESS
    assert bytes(sock.sent) == b"\x8a\x02hi"


def test_websocket_control_reply_precedes_new_mqtt_frame():
    sock = _PartialWebSocket(incoming=b"\x89\x02hi", chunk=2)
    wrapper = _wrapper_for_io(sock)

    with pytest.raises(BlockingIOError):
        wrapper.recv(65536)
    for _ in range(100):
        if wrapper.send(b"mqtt") == 4:
            break

    assert bytes(sock.sent).startswith(b"\x8a\x02hi")
    assert _unmask_client_frame(sock.sent[4:]) == b"mqtt"


def test_websocket_partial_close_reply_is_lossless():
    sock = _PartialWebSocket(incoming=b"\x88\x02\x03\xe8", chunk=2)
    wrapper = _wrapper_for_io(sock)

    with pytest.raises(BlockingIOError):
        wrapper.recv(65536)
    while wrapper.pending_write():
        wrapper.flush()

    assert bytes(sock.sent) == b"\x88\x02\x03\xe8"


@pytest.mark.parametrize(
    "stream",
    [
        b"\x80\x01x",  # continuation without an open fragmented message
        b"\x09\x01x",  # fragmented control frame
        b"\x82\x81\x00\x00\x00\x00x",  # masked server frame
    ],
)
def test_websocket_rejects_invalid_server_frames(stream):
    wrapper = _wrapper_for_io(_PartialWebSocket(incoming=stream, chunk=100))

    assert wrapper.recv(65536) == b""
    assert wrapper.connected is False


class TestHeaders:
    """ Make sure headers are used correctly """

    @pytest.mark.parametrize("wargs,expected_sent", [
        (
            # HTTPS on non-default port
            {
                "host": "testhost.com",
                "port": 1234,
                "path": "/mqtt",
                "extra_headers": None,
                "is_ssl": True,
            },
            [
                "GET /mqtt HTTP/1.1",
                "Host: testhost.com:1234",
                "Upgrade: websocket",
                "Connection: Upgrade",
                "Sec-Websocket-Protocol: mqtt",
                "Sec-Websocket-Version: 13",
                "Origin: https://testhost.com:1234",
            ],
        ),
        (
            # HTTPS on default port
            {
                "host": "testhost.com",
                "port": 443,
                "path": "/mqtt",
                "extra_headers": None,
                "is_ssl": True,
            },
            [
                "GET /mqtt HTTP/1.1",
                "Host: testhost.com",
                "Upgrade: websocket",
                "Connection: Upgrade",
                "Sec-Websocket-Protocol: mqtt",
                "Sec-Websocket-Version: 13",
                "Origin: https://testhost.com",
            ],
        ),
        (
            # HTTP on default port
            {
                "host": "testhost.com",
                "port": 80,
                "path": "/mqtt",
                "extra_headers": None,
                "is_ssl": False,
            },
            [
                "GET /mqtt HTTP/1.1",
                "Host: testhost.com",
                "Upgrade: websocket",
                "Connection: Upgrade",
                "Sec-Websocket-Protocol: mqtt",
                "Sec-Websocket-Version: 13",
                "Origin: http://testhost.com",
            ],
        ),
        (
            # HTTP on non-default port
            {
                "host": "testhost.com",
                "port": 443,  # This isn't the default *HTTP* port. It's on purpose to use httpS port
                "path": "/mqtt",
                "extra_headers": None,
                "is_ssl": False,
            },
            [
                "GET /mqtt HTTP/1.1",
                "Host: testhost.com:443",
                "Upgrade: websocket",
                "Connection: Upgrade",
                "Sec-Websocket-Protocol: mqtt",
                "Sec-Websocket-Version: 13",
                "Origin: http://testhost.com:443",
            ],
        ),
    ])
    def test_normal_headers(self, wargs, expected_sent):
        """ Normal headers as specified in RFC 6455 """

        response = [
            "HTTP/1.1 101 Switching Protocols",
            "Upgrade: websocket",
            "Connection: Upgrade",
            "Sec-WebSocket-Accept: badreturnvalue=",
            "Sec-WebSocket-Protocol: chat",
            "\r\n",
        ]

        def iter_response():
            for i in "\r\n".join(response).encode("utf8"):
                yield i

            for i in b"\r\n":
                yield i

        it = iter_response()

        def fakerecv(*args):
            return bytes([next(it)])

        mocksock = Mock(
            spec_set=socket.socket,
            recv=fakerecv,
            send=Mock(),
        )

        # Do a copy to avoid modifying input
        wargs_with_socket = dict(wargs)
        wargs_with_socket["socket"] = mocksock

        with pytest.raises(WebsocketConnectionError) as exc:
            _WebsocketWrapper(**wargs_with_socket)

        # We're not creating the response hash properly so it should raise this
        # error
        assert str(exc.value) == "WebSocket handshake error, invalid secret key"

        # Only sends the header once
        assert mocksock.send.call_count == 1

        got_lines = mocksock.send.call_args[0][0].decode("utf8").splitlines()

        # First line must be the GET line
        # 2nd line is required to be Host (rfc9110 said that it SHOULD be first header)
        assert expected_sent[0] == got_lines[0]
        assert expected_sent[1] == got_lines[1]

        # Other line order don't matter
        for line in expected_sent:
            assert line in got_lines
