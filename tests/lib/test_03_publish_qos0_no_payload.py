# Test whether a client sends a correct PUBLISH to a topic with QoS 0 and no payload.

# The client should connect with keepalive=60, clean session set,
# and client id publish-qos0-test-np
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK and verifying that rc=0, the client should send a PUBLISH message
# to topic "pub/qos0/no-payload/test" with zero length payload and QoS=0. If
# rc!=0, the client should exit with an error.
# After sending the PUBLISH message, the client should send a DISCONNECT message.


from tests import paho_test

connect_packet = paho_test.gen_connect("publish-qos0-test-np", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

publish_packet = paho_test.gen_publish("pub/qos0/no-payload/test", qos=0)

disconnect_packet = paho_test.gen_disconnect()


def test_03_publish_qos0_no_payload(server_socket, start_client):
    start_client("03-publish-qos0-no-payload.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "publish", publish_packet)
    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
