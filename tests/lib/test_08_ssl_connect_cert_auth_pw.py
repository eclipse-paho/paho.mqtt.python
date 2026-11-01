# Test whether a client produces a correct connect and subsequent disconnect when using SSL.
# Client must provide a certificate - the private key is encrypted with a password.
#
# The client should connect with keepalive=60, clean session set,
# and client id 08-ssl-connect-crt-auth
# It should use the CA certificate ssl/all-ca.crt for verifying the server.
# The test will send a CONNACK message to the client with rc=0. Upon receiving
# the CONNACK and verifying that rc=0, the client should send a DISCONNECT
# message. If rc!=0, the client should exit with an error.

from tests import paho_test

connect_packet = paho_test.gen_connect("08-ssl-connect-crt-auth-pw", keepalive=60)
connack_packet = paho_test.gen_connack(rc=0)
disconnect_packet = paho_test.gen_disconnect()


def test_08_ssl_connect_crt_auth_pw(ssl_server_socket, start_client):
    start_client("08-ssl-connect-cert-auth-pw.py")

    (conn, _address) = ssl_server_socket.accept()
    conn.settimeout(10)

    paho_test.expect_packet(conn, "connect", connect_packet)
    conn.send(connack_packet)

    paho_test.expect_packet(conn, "disconnect", disconnect_packet)

    conn.close()
