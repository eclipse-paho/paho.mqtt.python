# Test whether a client produces a correct connect with a unicode username and password.

# The client should connect with keepalive=60, clean session set,
# client id 01-unpwd-unicode-set, username and password from corresponding variables


from tests import paho_test

connect_packet = paho_test.gen_connect(
    "01-unpwd-unicode-set",
    keepalive=60,
    username="\u00fas\u00e9rn\u00e1m\u00e9-h\u00e9ll\u00f3",
    password="h\u00e9ll\u00f3",
)


def test_01_unpwd_unicode_set(server_socket, start_client):
    start_client("01-unpwd-unicode-set.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)

    conn.close()
