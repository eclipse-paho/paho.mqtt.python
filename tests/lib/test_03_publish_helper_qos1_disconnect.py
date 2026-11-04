# Test whether a client sends a correct PUBLISH to a topic with QoS 1,
# then responds correctly to a disconnect.
# Use paho.mqtt.publish helper for that.


from tests import paho_test

connect_packet = paho_test.gen_connect(
    "publish-helper-qos1-disconnect-test", keepalive=60,
)
connack_packet = paho_test.gen_connack(rc=0)

mid = 1
publish_packet = paho_test.gen_publish(
    "pub/qos1/test", qos=1, mid=mid, payload="message"
)
publish_packet_dup = paho_test.gen_publish(
    "pub/qos1/test", qos=1, mid=mid, payload="message",
    dup=True,
)
puback_packet = paho_test.gen_puback(mid)

disconnect_packet = paho_test.gen_disconnect()


def test_03_publish_helper_qos1_disconnect(server_socket, start_client):
    start_client("03-publish-helper-qos1-disconnect.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "publish", publish_packet)
    # Disconnect client. It should reconnect.
    conn.close()

    (conn, _address) = server_socket.accept()
    conn.settimeout(15)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "retried publish", publish_packet_dup)
    conn.send(puback_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
