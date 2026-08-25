import socket
from unittest.mock import Mock

import pytest
from paho.mqtt.client import WebsocketConnectionError, _WebsocketWrapper, websockets_connect


@pytest.mark.skipif(
    websockets_connect is not None,
    reason="Only class under test: _WebsocketWrapper is not used when websockets is installed."
)
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
