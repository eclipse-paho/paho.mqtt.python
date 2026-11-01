# Test whether a client sends a correct retained PUBLISH to a topic with QoS 0.


from tests import paho_test

mid = 16
connect_packet = paho_test.gen_connect("retain-qos0-test", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

publish_packet = paho_test.gen_publish(
    "retain/qos0/test", qos=0, payload="retained message", retain=True)


def test_04_retain_qos0(server_socket, start_client):
    start_client("04-retain-qos0.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "publish", publish_packet)

    conn.close()
