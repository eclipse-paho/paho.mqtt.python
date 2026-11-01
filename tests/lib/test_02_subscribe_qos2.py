# Test whether a client sends a correct SUBSCRIBE to a topic with QoS 2.

# The client should connect with keepalive=60, clean session set,
# and client id subscribe-qos2-test
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK and verifying that rc=0, the client should send a SUBSCRIBE
# message to subscribe to topic "qos2/test" with QoS=2. If rc!=0, the client
# should exit with an error.
# Upon receiving the correct SUBSCRIBE message, the test will reply with a
# SUBACK message with the accepted QoS set to 2. On receiving the SUBACK
# message, the client should send a DISCONNECT message.


from tests import paho_test

connect_packet = paho_test.gen_connect("subscribe-qos2-test", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

mid = 1
subscribe_packet = paho_test.gen_subscribe(mid, "qos2/test", 2)
suback_packet = paho_test.gen_suback(mid, 2)


def test_02_subscribe_qos2(server_socket, start_client):
    start_client("02-subscribe-qos2.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "subscribe", subscribe_packet)
    conn.send(suback_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
