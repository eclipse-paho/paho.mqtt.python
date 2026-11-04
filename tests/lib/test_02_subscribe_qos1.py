# Test whether a client sends a correct SUBSCRIBE to a topic with QoS 1.

# The client should connect with keepalive=60, clean session set,
# and client id subscribe-qos1-test
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK and verifying that rc=0, the client should send a SUBSCRIBE
# message to subscribe to topic "qos1/test" with QoS=1. If rc!=0, the client
# should exit with an error.
# Upon receiving the correct SUBSCRIBE message, the test will reply with a
# SUBACK message with the accepted QoS set to 1. On receiving the SUBACK
# message, the client should send a DISCONNECT message.


from tests import paho_test

connect_packet = paho_test.gen_connect("subscribe-qos1-test", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

mid = 1
subscribe_packet = paho_test.gen_subscribe(mid, "qos1/test", 1)
suback_packet = paho_test.gen_suback(mid, 1)


def test_02_subscribe_qos1(server_socket, start_client):
    start_client("02-subscribe-qos1.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "subscribe", subscribe_packet)
    conn.send(suback_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
