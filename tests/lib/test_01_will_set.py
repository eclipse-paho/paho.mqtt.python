# Test whether a client produces a correct connect with a will.
# Will QoS=1, will retain=1.

# The client should connect with keepalive=60, clean session set,
# client id 01-will-set will topic set to topic/on/unexpected/disconnect , will
# payload set to "will message", will qos set to 1 and will retain set.


from tests import paho_test

connect_packet = paho_test.gen_connect(
    "01-will-set", keepalive=60, will_topic="topic/on/unexpected/disconnect",
    will_qos=1, will_retain=True, will_payload="will message")


def test_01_will_set(server_socket, start_client):
    start_client("01-will-set.py")
    (conn, _address) = server_socket.accept()
    conn.settimeout(10)
    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.close()
