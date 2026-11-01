# Test whether asyncio works


from tests import paho_test

connect_packet = paho_test.gen_connect("asyncio-test", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)

subscribe_packet = paho_test.gen_subscribe(mid=1, topic="sub-test", qos=1)
suback_packet = paho_test.gen_suback(mid=1, qos=1)

unsubscribe_packet = paho_test.gen_unsubscribe(mid=2, topic="unsub-test")
unsuback_packet = paho_test.gen_unsuback(mid=2)

publish_packet = paho_test.gen_publish("b2c", qos=0, payload="msg")

publish_packet_in = paho_test.gen_publish("asyncio", qos=1, mid=3, payload="message")
puback_packet_in = paho_test.gen_puback(mid=3)

disconnect_packet = paho_test.gen_disconnect()


def test_01_asyncio(server_socket, start_client):
    proc = start_client("01-asyncio.py")

    (conn, _address) = server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "subscribe", subscribe_packet)
    conn.send(suback_packet)

    paho_test.expect_packet(conn, "unsubscribe", unsubscribe_packet)
    conn.send(unsuback_packet)
    conn.send(publish_packet)

    paho_test.expect_packet(conn, "publish", publish_packet_in)
    conn.send(puback_packet_in)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
    assert proc.wait() == 0
