import pytest

from tests.paho_test import ssl


def test_08_ssl_fake_cacert(ssl_server_socket, start_client):
    start_client("08-ssl-fake-cacert.py")
    with pytest.raises(ssl.SSLError):
        (conn, _address) = ssl_server_socket.accept()
        conn.close()
