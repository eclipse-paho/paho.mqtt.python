# Test whether a client produces a correct connect with a username and password.

# The client should connect with keepalive=60, clean session set,
# client id 01-unpwd-set, username and password set to empty string.


from tests import paho_test

connect_packet = paho_test.gen_connect(
    "01-unpwd-set", keepalive=60, username="", password='')


def test_01_unpwd_empty_set(server_socket, start_client):
    start_client("01-unpwd-empty-set.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)

    conn.close()
