# Test whether a client produces a correct connect with clean session not set.

# The client should connect with keepalive=60, clean session not
# set, and client id 01-no-clean-session.


from tests import paho_test

connect_packet = paho_test.gen_connect("01-no-clean-session", clean_session=False, keepalive=60)


def test_01_no_clean_session(server_socket, start_client):
    start_client("01-no-clean-session.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)

    conn.close()
