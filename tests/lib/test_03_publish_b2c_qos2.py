# Test whether a client responds correctly to a PUBLISH with QoS 1.

# The client should connect with keepalive=60, clean session set,
# and client id publish-qos1-test
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK the client should verify that rc==0.
# The test will send the client a PUBLISH message with topic
# "pub/qos1/receive", payload of "message", QoS=1 and mid=123. The client
# should handle this as per the spec by sending a PUBACK message.
# The client should then exit with return code==0.
from tests import paho_test

connect_packet = paho_test.gen_connect("publish-qos2-test", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

mid = 13423
publish_packet = paho_test.gen_publish(
    "pub/qos2/receive", qos=2, mid=mid, payload="message")
pubrec_packet = paho_test.gen_pubrec(mid=mid)
pubrel_packet = paho_test.gen_pubrel(mid=mid)
pubcomp_packet = paho_test.gen_pubcomp(mid)


def test_03_publish_b2c_qos2(server_socket, start_client):
    start_client("03-publish-b2c-qos2.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)
    conn.send(publish_packet)

    paho_test.expect_packet(conn, "pubrec", pubrec_packet)
    conn.send(pubrel_packet)

    paho_test.expect_packet(conn, "pubcomp", pubcomp_packet)

    conn.close()
