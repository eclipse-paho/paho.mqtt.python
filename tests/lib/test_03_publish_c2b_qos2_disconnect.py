# Test whether a client sends a correct PUBLISH to a topic with QoS 2 and responds to a disconnect.


from tests import paho_test

connect_packet = paho_test.gen_connect(
    "publish-qos2-test", keepalive=60, clean_session=False,
)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

mid = 1
publish_packet = paho_test.gen_publish(
    "pub/qos2/test", qos=2, mid=mid, payload="message")
publish_dup_packet = paho_test.gen_publish(
    "pub/qos2/test", qos=2, mid=mid, payload="message", dup=True)
pubrec_packet = paho_test.gen_pubrec(mid)
pubrel_packet = paho_test.gen_pubrel(mid)
pubcomp_packet = paho_test.gen_pubcomp(mid)


def test_03_publish_c2b_qos2_disconnect(server_socket, start_client):
    start_client("03-publish-c2b-qos2-disconnect.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(5)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "publish", publish_packet)
    # Disconnect client. It should reconnect.
    conn.close()

    (conn, _address) = server_socket.accept()
    conn.settimeout(15)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "retried publish", publish_dup_packet)
    conn.send(pubrec_packet)

    paho_test.expect_packet(conn, "pubrel", pubrel_packet)
    # Disconnect client. It should reconnect.
    conn.close()

    (conn, _address) = server_socket.accept()
    conn.settimeout(15)

    # Complete connection and message flow.
    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "retried pubrel", pubrel_packet)
    conn.send(pubcomp_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
