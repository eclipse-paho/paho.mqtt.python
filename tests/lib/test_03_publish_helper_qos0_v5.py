# Test whether a client sends a correct PUBLISH to a topic with QoS 0.
# Use paho.mqtt.publish helper for that.

# The client should connect with keepalive=60, clean session set,
# and client id publish-helper-qos0-test
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK and verifying that rc=0, the client should send a PUBLISH message
# to topic "pub/qos0/test" with payload "message" and QoS=0. If rc!=0, the
# client should exit with an error.
# After sending the PUBLISH message, the client should send a
# DISCONNECT message.


from tests import paho_test

connect_packet = paho_test.gen_connect(
    "publish-helper-qos0-test", keepalive=60, proto_ver=5, properties=None
)
connack_packet = paho_test.gen_connack(rc=0, proto_ver=5)

publish_packet = paho_test.gen_publish(
    "pub/qos0/test", qos=0, payload="message", proto_ver=5
)

disconnect_packet = paho_test.gen_disconnect()


def test_03_publish_helper_qos0_v5(server_socket, start_client):
    start_client("03-publish-helper-qos0-v5.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "publish", publish_packet)
    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
