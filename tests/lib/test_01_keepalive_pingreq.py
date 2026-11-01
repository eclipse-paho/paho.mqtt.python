# Test whether a client sends a pingreq after the keepalive time

# The client should connect with keepalive=4, clean session set,
# and client id 01-keepalive-pingreq
# The client should send a PINGREQ message after the appropriate amount of time
# (4 seconds after no traffic).

import time

from tests import paho_test

connect_packet = paho_test.gen_connect("01-keepalive-pingreq", keepalive=4)
connack_packet = paho_test.gen_connack(rc=0)

pingreq_packet = paho_test.gen_pingreq()
pingresp_packet = paho_test.gen_pingresp()


def test_01_keepalive_pingreq(server_socket, start_client):
    start_client("01-keepalive-pingreq.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "pingreq", pingreq_packet)
    time.sleep(1.0)
    conn.send(pingresp_packet)

    paho_test.expect_packet(conn, "pingreq", pingreq_packet)
