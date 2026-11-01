# Test whether a client connects correctly with a zero length clientid.


from tests import paho_test

connect_packet = paho_test.gen_connect("", keepalive=60, proto_ver=4)
connack_packet = paho_test.gen_connack(rc=0)

disconnect_packet = paho_test.gen_disconnect()


def test_01_zero_length_clientid(server_socket, start_client):
    start_client("01-zero-length-clientid.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
