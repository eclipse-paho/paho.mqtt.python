# Test whether a client produces a correct connect and subsequent disconnect when using SSL.
# Client must provide a certificate.
#
# The client should connect with keepalive=60, clean session set,
# and client id 08-ssl-connect-alpn
# It should use the CA certificate ssl/all-ca.crt for verifying the server.
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK and verifying that rc=0, the client should send a DISCONNECT
# message. If rc!=0, the client should exit with an error.
#
# Additionally, the secure socket must have been negotiated with the "paho-test-protocol"


from tests import paho_test
from tests.paho_test import ssl


def test_08_ssl_connect_alpn(alpn_ssl_server_socket, start_client):
    connect_packet = paho_test.gen_connect("08-ssl-connect-alpn", keepalive=60)
    connack_packet = paho_test.gen_connack(rc=0)
    disconnect_packet = paho_test.gen_disconnect()

    start_client("08-ssl-connect-alpn.py")

    (conn, _address) = alpn_ssl_server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    if ssl.HAS_ALPN:
        negotiated_protocol = conn.selected_alpn_protocol()
        if negotiated_protocol != "paho-test-protocol":
            raise Exception(f"Unexpected protocol '{negotiated_protocol}'")

    conn.close()
