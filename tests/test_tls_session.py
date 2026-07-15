import paho.mqtt.client as mqtt
import pytest


class FakeSSLSocket:
    def __init__(
        self,
        session=None,
        session_reused=True,
        version="TLSv1.3",
        tcp_nodelay=False,
    ):
        self.session = session
        self.session_reused = session_reused
        self._version = version
        self.tcp_nodelay = tcp_nodelay
        self.closed = False
        self.timeout = None
        self.handshakes = 0

    def settimeout(self, timeout):
        self.timeout = timeout

    def do_handshake(self):
        self.handshakes += 1

    def close(self):
        self.closed = True

    def version(self):
        return self._version

    def getsockopt(self, level, option):
        assert level == mqtt.socket.IPPROTO_TCP
        assert option == mqtt.socket.TCP_NODELAY
        return int(self.tcp_nodelay)


class FakeContext:
    check_hostname = True

    def __init__(self, result=None):
        self.result = result or FakeSSLSocket()
        self.calls = []

    def wrap_socket(self, sock, **kwargs):
        self.calls.append((sock, kwargs))
        if hasattr(sock, "tcp_nodelay"):
            self.result.tcp_nodelay = sock.tcp_nodelay
        return self.result


class FakeRawSocket:
    def __init__(self, tcp_nodelay=False):
        self.tcp_nodelay = tcp_nodelay

    def getsockopt(self, level, option):
        assert level == mqtt.socket.IPPROTO_TCP
        assert option == mqtt.socket.TCP_NODELAY
        return int(self.tcp_nodelay)


def tls_client(context):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client._ssl = True
    client._ssl_context = context
    client._tls_insecure = False
    client._host = "broker.example"
    client._port = 8883
    return client


def test_tls_defaults_use_modern_client_context():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    client.tls_set()

    assert client._ssl is True
    assert client._ssl_context.protocol == mqtt.ssl.PROTOCOL_TLS_CLIENT
    assert client._ssl_context.verify_mode == mqtt.ssl.CERT_REQUIRED
    assert client._ssl_context.check_hostname is True
    assert client._tls_insecure is False

    client.tls_insecure_set(True)
    assert client._ssl_context.check_hostname is False
    assert client._tls_insecure is True


def test_tls_cert_none_disables_hostname_check_before_verify_mode():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    client.tls_set(cert_reqs=mqtt.ssl.CERT_NONE)

    assert client._ssl_context.protocol == mqtt.ssl.PROTOCOL_TLS_CLIENT
    assert client._ssl_context.verify_mode == mqtt.ssl.CERT_NONE
    assert client._ssl_context.check_hostname is False
    assert client._tls_insecure is True


def test_tls_set_still_rejects_build_without_ssl(monkeypatch):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    monkeypatch.setattr(mqtt, "ssl", None)

    with pytest.raises(ValueError, match="no SSL/TLS"):
        client.tls_set()


def test_close_caches_session_and_matching_target_reuses_it(monkeypatch):
    monkeypatch.setattr(mqtt.ssl, "SSLSocket", FakeSSLSocket)
    context = FakeContext()
    client = tls_client(context)
    session = object()
    connected = FakeSSLSocket(session)
    client._sock = connected
    client._tls_socket_session_key = client._tls_session_target()
    client._tls_socket_session_protocol = "TLSv1.3"

    client._sock_close()

    assert connected.closed
    assert client._tls_session is session
    raw = object()
    wrapped = client._ssl_wrap_socket(raw)
    assert wrapped is context.result
    assert context.calls[-1][1]["session"] is session
    assert context.calls[-1][1]["server_hostname"] == "broker.example"
    assert wrapped.handshakes == 1


@pytest.mark.parametrize("changed", ("context", "host", "port", "transport"))
def test_target_or_context_change_discards_cached_session(changed):
    first_context = FakeContext()
    client = tls_client(first_context)
    client._tls_session = object()
    client._tls_session_key = client._tls_session_target()
    second_context = FakeContext()
    if changed == "context":
        client._ssl_context = second_context
    elif changed == "host":
        client._host = "other.example"
    elif changed == "port":
        client._port += 1
    else:
        client._transport = "websockets"

    client._ssl_wrap_socket(object())

    assert client._tls_session is None
    assert "session" not in client._ssl_context.calls[-1][1]


def test_websocket_close_caches_underlying_tls_session(monkeypatch):
    monkeypatch.setattr(mqtt.ssl, "SSLSocket", FakeSSLSocket)
    client = tls_client(FakeContext())
    client._transport = "websockets"
    session = object()
    wrapper = object.__new__(mqtt._WebsocketWrapper)
    wrapper._socket = FakeSSLSocket(session)
    client._tls_socket_session_key = client._tls_session_target()
    client._tls_socket_session_protocol = "TLSv1.3"

    client._cache_tls_session(wrapper)

    assert client._tls_session is session
    assert client._tls_session_key == client._tls_session_target()


def test_insecure_connection_does_not_cache_session(monkeypatch):
    monkeypatch.setattr(mqtt.ssl, "SSLSocket", FakeSSLSocket)
    client = tls_client(FakeContext())
    client._tls_insecure = True

    client._cache_tls_session(FakeSSLSocket(object()))

    assert client._tls_session is None


def test_tls12_session_is_not_cached_or_reused_without_tcp_nodelay(monkeypatch):
    monkeypatch.setattr(mqtt.ssl, "SSLSocket", FakeSSLSocket)
    session = object()
    context = FakeContext(FakeSSLSocket(session, version="TLSv1.2"))
    client = tls_client(context)
    wrapped = client._ssl_wrap_socket(FakeRawSocket())
    client._cache_tls_session(wrapped)
    context.result = FakeSSLSocket(version="TLSv1.2")

    client._ssl_wrap_socket(FakeRawSocket())

    assert client._tls_session is None
    assert "session" not in context.calls[-1][1]


def test_tls12_session_is_reused_with_tcp_nodelay(monkeypatch):
    monkeypatch.setattr(mqtt.ssl, "SSLSocket", FakeSSLSocket)
    session = object()
    context = FakeContext(FakeSSLSocket(session, version="TLSv1.2"))
    client = tls_client(context)
    wrapped = client._ssl_wrap_socket(FakeRawSocket(tcp_nodelay=True))
    client._cache_tls_session(wrapped)
    context.result = FakeSSLSocket(version="TLSv1.2")

    client._ssl_wrap_socket(FakeRawSocket(tcp_nodelay=True))

    assert context.calls[-1][1]["session"] is session


def test_local_session_mismatch_requests_fresh_connection():
    class RejectingContext(FakeContext):
        def wrap_socket(self, sock, **kwargs):
            self.calls.append((sock, kwargs))
            raise ValueError("session belongs to a different context")

    context = RejectingContext()
    client = tls_client(context)
    client._tls_session = object()
    client._tls_session_key = client._tls_session_target()

    with pytest.raises(mqtt._RetryTLSWithoutSession):
        client._ssl_wrap_socket(object())

    assert client._tls_session is None
    assert len(context.calls) == 2


def test_server_rejection_disables_further_attempts_for_target(monkeypatch):
    monkeypatch.setattr(mqtt.ssl, "SSLSocket", FakeSSLSocket)
    context = FakeContext(FakeSSLSocket(session_reused=False))
    client = tls_client(context)
    client._tls_session = object()
    client._tls_session_key = client._tls_session_target()

    client._ssl_wrap_socket(object())

    assert client._tls_session is None
    assert client._tls_session_disabled_key == client._tls_session_target()
    client._cache_tls_session(FakeSSLSocket(object()))
    assert client._tls_session is None


def test_certificate_error_clears_cached_session():
    class RejectingContext(FakeContext):
        def wrap_socket(self, sock, **kwargs):
            raise mqtt.ssl.CertificateError("hostname mismatch")

    context = RejectingContext()
    client = tls_client(context)
    client._tls_session = object()
    client._tls_session_key = client._tls_session_target()

    with pytest.raises(mqtt.ssl.CertificateError):
        client._ssl_wrap_socket(object())

    assert client._tls_session is None


def test_create_socket_retries_once_without_session(monkeypatch):
    client = tls_client(FakeContext())
    first = FakeSSLSocket()
    second = FakeSSLSocket()
    wrapped = FakeSSLSocket()
    raw_sockets = iter((first, second))
    calls = []

    monkeypatch.setattr(client, "_create_raw_socket", lambda: next(raw_sockets))

    def wrap(sock, use_cached_session=True):
        calls.append((sock, use_cached_session))
        if use_cached_session:
            raise mqtt._RetryTLSWithoutSession()
        return wrapped

    monkeypatch.setattr(client, "_ssl_wrap_socket", wrap)

    assert client._create_socket() is wrapped
    assert first.closed
    assert calls == [(first, True), (second, False)]
