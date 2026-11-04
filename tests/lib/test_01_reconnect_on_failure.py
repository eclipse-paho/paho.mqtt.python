# Test the reconnect_on_failure = False mode
import pytest

from tests import paho_test

connect_packet = paho_test.gen_connect("01-reconnect-on-failure", keepalive=60)
connack_packet_ok = paho_test.gen_connack(rc=0)
connack_packet_failure = paho_test.gen_connack(rc=1)  # CONNACK_REFUSED_PROTOCOL_VERSION

publish_packet = paho_test.gen_publish(
    "reconnect/test", qos=0, payload="message")


@pytest.mark.parametrize("ok_code", [False, True])
def test_01_reconnect_on_failure(server_socket, start_client, ok_code):
    client = start_client("01-reconnect-on-failure.py", expected_returncode=42)

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    if ok_code:
        conn.send(connack_packet_ok)
        # Connection is a success, so we expect a publish
        paho_test.expect_packet(conn, "publish", publish_packet)
    else:
        conn.send(connack_packet_failure)
    conn.close()
    # Expect the client to quit here due to socket being closed
    client.wait(1)
    assert client.returncode == 42
