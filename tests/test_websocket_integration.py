import base64
import hashlib
import re
import socketserver
import socket
from collections import OrderedDict

import paho.mqtt.client as client
import pytest
from paho.mqtt.client import WebsocketConnectionError

from tests.testsupport.broker import fake_websocket_broker  # noqa: F401


@pytest.fixture
def init_response_headers():
    # "Normal" websocket response from server
    response_headers = OrderedDict([
        ("Upgrade", "websocket"),
        ("Connection", "Upgrade"),
        ("Sec-WebSocket-Accept", "testwebsocketkey"),
        ("Sec-WebSocket-Protocol", "chat"),
    ])

    return response_headers


def get_websocket_response(response_headers):
    """ Takes headers and constructs HTTP response

    'HTTP/1.1 101 Switching Protocols' is the headers for the response,
    as expected in client.py
    """
    response = "\r\n".join([
        "HTTP/1.1 101 Switching Protocols",
        "\r\n".join(f"{i}: {j}" for i, j in response_headers.items()),
        "\r\n",
    ]).encode("utf8")

    return response


@pytest.mark.parametrize("proto_ver,proto_name", [
    (client.MQTTv31, "MQIsdp"),
    (client.MQTTv311, "MQTT"),
])
class TestInvalidWebsocketResponse:
    def test_unexpected_response(self, proto_ver, proto_name, fake_websocket_broker):
        """ Server responds with a valid code, but it's not what the client expected """

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION1,
            "test_unexpected_response",
            protocol=proto_ver,
            transport="websockets"
            )

        class WebsocketHandler(socketserver.BaseRequestHandler):
            def handle(_self):
                # Respond with data passed in to serve()
                _self.request.sendall(b"200 OK")

        with fake_websocket_broker.serve(WebsocketHandler), pytest.raises(WebsocketConnectionError) as exc:
            mqttc.connect("localhost", fake_websocket_broker.port, keepalive=10)

        assert str(exc.value) == "WebSocket handshake error"


@pytest.mark.parametrize("proto_ver,proto_name", [
    (client.MQTTv31, "MQIsdp"),
    (client.MQTTv311, "MQTT"),
])
class TestBadWebsocketHeaders:
    """ Testing for basic functionality in checking for headers """

    def _get_basic_handler(self, response_headers):
        """ Get a basic BaseRequestHandler which returns the information in
        self._response_headers
        """

        response = get_websocket_response(response_headers)

        class WebsocketHandler(socketserver.BaseRequestHandler):
            def handle(_self):
                self.data = _self.request.recv(1024).strip()
                print('Received', self.data.decode('utf8'))
                # Respond with data passed in to serve()
                _self.request.sendall(response)

        return WebsocketHandler

    def test_no_upgrade(self, proto_ver, proto_name, fake_websocket_broker,
                        init_response_headers):
        """ Server doesn't respond with 'connection: upgrade' """

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION1,
            "test_no_upgrade",
            protocol=proto_ver,
            transport="websockets"
            )

        init_response_headers["Connection"] = "bad"
        response = self._get_basic_handler(init_response_headers)

        with fake_websocket_broker.serve(response), pytest.raises(WebsocketConnectionError) as exc:
            mqttc.connect("localhost", fake_websocket_broker.port, keepalive=10)

        assert str(exc.value) == "WebSocket handshake error, connection not upgraded"

    def test_bad_secret_key(self, proto_ver, proto_name, fake_websocket_broker,
                            init_response_headers):
        """ Server doesn't give anything after connection: upgrade """

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION1,
            "test_bad_secret_key",
            protocol=proto_ver,
            transport="websockets"
            )

        response = self._get_basic_handler(init_response_headers)

        with fake_websocket_broker.serve(response), pytest.raises(WebsocketConnectionError) as exc:
            mqttc.connect("localhost", fake_websocket_broker.port, keepalive=10)

        assert str(exc.value) == "WebSocket handshake error, invalid secret key"


@pytest.mark.parametrize("proto_ver,proto_name", [
    (client.MQTTv31, "MQIsdp"),
    (client.MQTTv311, "MQTT"),
])
class TestValidHeaders:
    """ Testing for functionality in request/response headers """

    def _get_callback_handler(self, response_headers, check_request=None):
        """ Get a basic BaseRequestHandler which returns the information in
        self._response_headers
        """

        class WebsocketHandler(socketserver.BaseRequestHandler):
            def handle(_self):
                self.data = _self.request.recv(1024).strip()
                print('Received', self.data.decode('utf8'))

                decoded = self.data.decode("utf8")

                if check_request is not None:
                    check_request(decoded)

                # Create server hash
                GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
                key = re.search("sec-websocket-key: ([A-Za-z0-9+/=]*)", decoded, re.IGNORECASE).group(1)

                to_hash = f"{key:s}{GUID:s}"
                hashed = hashlib.sha1(to_hash.encode("utf8"))  # noqa: S324
                encoded = base64.b64encode(hashed.digest()).decode("utf8")

                response_headers["Sec-WebSocket-Accept"] = encoded

                # Respond with the correct hash
                response = get_websocket_response(response_headers)

                _self.request.sendall(response)

        return WebsocketHandler

    def test_successful_connection(self, proto_ver, proto_name,
                                   fake_websocket_broker,
                                   init_response_headers):
        """ Connect successfully, on correct path """

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION1,
            "test_successful_connection",
            protocol=proto_ver,
            transport="websockets"
            )

        response = self._get_callback_handler(init_response_headers)

        with fake_websocket_broker.serve(response):
            mqttc.connect("localhost", fake_websocket_broker.port, keepalive=10)

            mqttc.socket().setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            mqttc.disconnect()

    @pytest.mark.parametrize("mqtt_path", [
        "/mqtt"
        "/special",
        None,
    ])
    def test_correct_path(self, proto_ver, proto_name, fake_websocket_broker,
                          mqtt_path, init_response_headers):
        """ Make sure it can connect on user specified paths """

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION1,
            "test_correct_path",
            protocol=proto_ver,
            transport="websockets"
            )

        mqttc.ws_set_options(
            path=mqtt_path,
        )

        def check_path_correct(decoded):
            # Make sure it connects to the right path
            if mqtt_path:
                assert re.search(f"GET {mqtt_path} HTTP/1.1", decoded, re.IGNORECASE) is not None

        response = self._get_callback_handler(
            init_response_headers,
            check_request=check_path_correct,
        )

        with fake_websocket_broker.serve(response):
            mqttc.connect("localhost", fake_websocket_broker.port, keepalive=10)

            mqttc.disconnect()

    @pytest.mark.parametrize("auth_headers", [
        {"Authorization": "test123"},
        {"Authorization": "test123", "auth2": "abcdef"},
        # Won't be checked, but make sure it still works even if the user passes it
        None,
    ])
    def test_correct_auth(self, proto_ver, proto_name, fake_websocket_broker,
                          auth_headers, init_response_headers):
        """ Make sure it sends the right auth headers """

        mqttc = client.Client(
            client.CallbackAPIVersion.VERSION1,
            "test_correct_path",
            protocol=proto_ver,
            transport="websockets"
            )

        mqttc.ws_set_options(
            headers=auth_headers,
        )

        def check_headers_used(decoded):
            # Make sure it connects to the right path
            if auth_headers:
                for k, v in auth_headers.items():
                    assert f"{k}: {v}" in decoded

        response = self._get_callback_handler(
            init_response_headers,
            check_request=check_headers_used,
        )

        with fake_websocket_broker.serve(response):
            mqttc.connect("localhost", fake_websocket_broker.port, keepalive=10)

            mqttc.disconnect()

