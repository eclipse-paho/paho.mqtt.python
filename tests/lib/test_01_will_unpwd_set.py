# Test whether a client produces a correct connect with a will, username and password.

# The client should connect with keepalive=60, clean session set,
# client id 01-will-unpwd-set , will topic set to "will-topic", will payload
# set to "will message", will qos=2, will retain not set, username set to
# "oibvvwqw" and password set to "#'^2hg9a&nm38*us".


from tests import paho_test

connect_packet = paho_test.gen_connect(
    "01-will-unpwd-set",
    keepalive=60, username="oibvvwqw", password="#'^2hg9a&nm38*us",
    will_topic="will-topic", will_qos=2, will_payload="will message",
)


def test_01_will_unpwd_set(server_socket, start_client):
    start_client("01-will-unpwd-set.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)

    conn.close()
