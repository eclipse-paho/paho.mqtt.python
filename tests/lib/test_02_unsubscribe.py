# Test whether a client sends a correct UNSUBSCRIBE packet.


from tests import paho_test

connect_packet = paho_test.gen_connect("unsubscribe-test", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()

mid = 1
unsubscribe_packet = paho_test.gen_unsubscribe(mid, "unsubscribe/test")
unsuback_packet = paho_test.gen_unsuback(mid)


def test_02_unsubscribe(server_socket, start_client):
    start_client("02-unsubscribe.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "unsubscribe", unsubscribe_packet)
    conn.send(unsuback_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
