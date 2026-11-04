# Test whether a client responds to max-inflight and reconnect when max-inflight is reached

# The client should connect with keepalive=60, clean session set,
# and client id publish-fill-inflight
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK the client should verify that rc==0.
# Then client should send 10 PUBLISH with QoS == 1. On client side 12 message will be
# submitted, so 2 will be queued.
# The test will wait 0.5 seconds after received the 10 PUBLISH. After this wait, it will
# disconnect the client.
# The client should re-connect and re-sent the first 10 messages.
# The test will PUBACK one message, it should receive another PUBLISH.
# The test will wait 0.5 seconds and expect no PUBLISH.
# The test will then PUBACK all message.
# The client should disconnect once everything is acked.

import pytest

from tests import paho_test


def expected_payload(i: int) -> bytes:
    return f"message{i}"

connect_packet = paho_test.gen_connect("publish-qos1-test", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

first_connection_publishes = [
    paho_test.gen_publish(
        "topic", qos=1, mid=i+1, payload=expected_payload(i),
    )
    for i in range(10)
]
second_connection_publishes = [
    paho_test.gen_publish(
        # I'm not sure we should have the mid+13.
        # Currently on reconnection client will do two wrong thing:
        # * it sent more than max_inflight packet
        # * it re-send message both with mid = old_mid + 12 AND with mid = old_mid & dup=1
        "topic", qos=1, mid=i+13, payload=expected_payload(i),
    )
    for i in range(12)
]
second_connection_pubacks = [
    paho_test.gen_puback(i+13)
    for i  in range(12)
]

@pytest.mark.xfail
def test_03_publish_fill_inflight(server_socket, start_client):
    start_client("03-publish-fill-inflight.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    for packet in first_connection_publishes:
        paho_test.expect_packet(conn, "publish", packet)

    paho_test.expect_no_packet(conn, 0.5)

    conn.close()

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    for packet in second_connection_publishes[:10]:
        paho_test.expect_packet(conn, "publish", packet)

    paho_test.expect_no_packet(conn, 0.2)

    conn.send(second_connection_pubacks[0])
    paho_test.expect_packet(conn, "publish", second_connection_publishes[10])

    paho_test.expect_no_packet(conn, 0.5)

    for packet in second_connection_pubacks[1:11]:
        conn.send(packet)

    paho_test.expect_packet(conn, "publish", second_connection_publishes[11])

    paho_test.expect_no_packet(conn, 0.5)

