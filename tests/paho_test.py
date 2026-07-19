import contextlib
import os
import socket
import struct
import time

from tests.consts import ssl_path
from tests.debug_helpers import dump_packet

try:
    import ssl
except ImportError:
    ssl = None

from tests import mqtt5_props


def bind_to_any_free_port(sock) -> int:
    """
    Bind a socket to an available port on localhost,
    and return the port number.
    """
    sock.bind(('localhost', 0))
    return sock.getsockname()[1]


def create_server_socket():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    port = bind_to_any_free_port(sock)
    sock.listen(5)
    return (sock, port)


def create_server_socket_ssl(*, path = ssl_path, verify_mode=None, alpn_protocols=None):
    assert ssl, "SSL not available"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_verify_locations(str(path / "all-ca.crt"))
    context.load_cert_chain(
        str(path / "server.crt"),
        str(path / "server.key"),
    )
    if verify_mode:
        context.verify_mode = verify_mode

    if alpn_protocols is not None:
        context.set_alpn_protocols(alpn_protocols)

    ssock = context.wrap_socket(sock, server_side=True)
    ssock.settimeout(10)
    port = bind_to_any_free_port(ssock)
    ssock.listen(5)
    return (ssock, port)


def expect_packet(sock, name, expected):
    rlen = len(expected) if len(expected) > 0 else 1

    packet_recvd = b""
    try:
        while len(packet_recvd) < rlen:
            data = sock.recv(rlen-len(packet_recvd))
            if len(data) == 0:
                break
            packet_recvd += data
    except socket.timeout:  # pragma: no cover
        pass

    assert packet_matches(name, packet_recvd, expected)
    return True


def expect_no_packet(sock, delay=1):
    """ expect that nothing is received within given delay
    """
    try:
        previous_timeout = sock.gettimeout()
        sock.settimeout(delay)
        data = sock.recv(1024)
    except socket.timeout:
        data = None
    finally:
        sock.settimeout(previous_timeout)

    if data is not None:
        dump_packet("Received unexpected", data)

    assert data is None, "shouldn't receive any data"


def packet_matches(name, recvd, expected):
    if recvd != expected:  # pragma: no cover
        print(f"FAIL: Received incorrect {name}.")
        dump_packet("Received", recvd)
        dump_packet("Expected", expected)
        return False
    else:
        return True


def gen_connect(
    client_id,
    clean_session=True,
    keepalive=60,
    username=None,
    password=None,
    will_topic=None,
    will_qos=0,
    will_retain=False,
    will_payload=b"",
    proto_ver=4,
    connect_reserved=False,
    properties=b"",
    will_properties=b"",
    session_expiry=-1,
):
    if (proto_ver&0x7F) == 3 or proto_ver == 0:
        remaining_length = 12
    elif (proto_ver&0x7F) == 4 or proto_ver == 5:
        remaining_length = 10
    else:
        raise ValueError

    if client_id is not None:
        client_id = client_id.encode("utf-8")
        remaining_length = remaining_length + 2+len(client_id)
    else:
        remaining_length = remaining_length + 2

    connect_flags = 0

    if connect_reserved:
        connect_flags = connect_flags | 0x01

    if clean_session:
        connect_flags = connect_flags | 0x02

    if proto_ver == 5:
        if properties == b"":
            properties += mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_RECEIVE_MAXIMUM, 20)

        if session_expiry != -1:
            properties += mqtt5_props.gen_uint32_prop(mqtt5_props.PROP_SESSION_EXPIRY_INTERVAL, session_expiry)

        properties = mqtt5_props.prop_finalise(properties)
        remaining_length += len(properties)

    if will_topic is not None:
        will_topic = will_topic.encode('utf-8')
        remaining_length = remaining_length + 2 + len(will_topic) + 2 + len(will_payload)
        connect_flags = connect_flags | 0x04 | ((will_qos & 0x03) << 3)
        if will_retain:
            connect_flags = connect_flags | 32
        if proto_ver == 5:
            will_properties = mqtt5_props.prop_finalise(will_properties)
            remaining_length += len(will_properties)

    if username is not None:
        username = username.encode('utf-8')
        remaining_length = remaining_length + 2 + len(username)
        connect_flags = connect_flags | 0x80
        if password is not None:
            password = password.encode('utf-8')
            connect_flags = connect_flags | 0x40
            remaining_length = remaining_length + 2 + len(password)

    rl = pack_remaining_length(remaining_length)
    packet = struct.pack("!B" + str(len(rl)) + "s", 0x10, rl)
    if (proto_ver&0x7F) == 3 or proto_ver == 0:
        packet = packet + struct.pack("!H6sBBH", len(b"MQIsdp"), b"MQIsdp", proto_ver, connect_flags, keepalive)
    elif (proto_ver&0x7F) == 4 or proto_ver == 5:
        packet = packet + struct.pack("!H4sBBH", len(b"MQTT"), b"MQTT", proto_ver, connect_flags, keepalive)

    if proto_ver == 5:
        packet += properties

    if client_id is not None:
        packet = packet + struct.pack("!H" + str(len(client_id)) + "s", len(client_id), bytes(client_id))
    else:
        packet = packet + struct.pack("!H", 0)

    if will_topic is not None:
        packet += will_properties
        packet = packet + struct.pack("!H" + str(len(will_topic)) + "s", len(will_topic), will_topic)
        if len(will_payload) > 0:
            packet = packet + struct.pack("!H" + str(len(will_payload)) + "s", len(will_payload), will_payload.encode('utf8'))
        else:
            packet = packet + struct.pack("!H", 0)

    if username is not None:
        packet = packet + struct.pack("!H" + str(len(username)) + "s", len(username), username)
        if password is not None:
            packet = packet + struct.pack("!H" + str(len(password)) + "s", len(password), password)
    return packet

def gen_connack(flags=0, rc=0, proto_ver=4, properties=b"", property_helper=True):
    if proto_ver == 5:
        if property_helper:
            if properties is not None:
                properties = mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_TOPIC_ALIAS_MAXIMUM, 10) \
                             + properties + mqtt5_props.gen_uint16_prop(mqtt5_props.PROP_RECEIVE_MAXIMUM, 20)
            else:
                properties = b""
        properties = mqtt5_props.prop_finalise(properties)

        packet = struct.pack('!BBBB', 32, 2+len(properties), flags, rc) + properties
    else:
        packet = struct.pack('!BBBB', 32, 2, flags, rc)

    return packet

def gen_publish(topic, qos, payload=None, retain=False, dup=False, mid=0, proto_ver=4, properties=b""):
    if isinstance(topic, str):
        topic = topic.encode("utf-8")
    rl = 2+len(topic)
    pack_format = "H"+str(len(topic))+"s"
    if qos > 0:
        rl = rl + 2
        pack_format = pack_format + "H"

    if proto_ver == 5:
        properties = mqtt5_props.prop_finalise(properties)
        rl += len(properties)
        # This will break if len(properties) > 127
        pack_format = pack_format + "%ds"%(len(properties))

    if payload is not None:
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        rl = rl + len(payload)
        pack_format = pack_format + str(len(payload)) + "s"
    else:
        payload = b""
        pack_format = pack_format + "0s"

    rlpacked = pack_remaining_length(rl)
    cmd = 48 | (qos << 1)
    if retain:
        cmd = cmd + 1
    if dup:
        cmd = cmd + 8

    if proto_ver == 5:
        if qos > 0:
            return struct.pack("!B" + str(len(rlpacked))+"s" + pack_format, cmd, rlpacked, len(topic), topic, mid, properties, payload)
        else:
            return struct.pack("!B" + str(len(rlpacked))+"s" + pack_format, cmd, rlpacked, len(topic), topic, properties, payload)
    else:
        if qos > 0:
            return struct.pack("!B" + str(len(rlpacked))+"s" + pack_format, cmd, rlpacked, len(topic), topic, mid, payload)
        else:
            return struct.pack("!B" + str(len(rlpacked))+"s" + pack_format, cmd, rlpacked, len(topic), topic, payload)

def _gen_command_with_mid(cmd, mid, proto_ver=4, reason_code=-1, properties=None):
    if proto_ver == 5 and (reason_code != -1 or properties is not None):
        if reason_code == -1:
            reason_code = 0

        if properties is None:
            return struct.pack('!BBHB', cmd, 3, mid, reason_code)
        elif properties == "":
            return struct.pack('!BBHBB', cmd, 4, mid, reason_code, 0)
        else:
            properties = mqtt5_props.prop_finalise(properties)
            pack_format = "!BBHB"+str(len(properties))+"s"
            return struct.pack(pack_format, cmd, 2+1+len(properties), mid, reason_code, properties)
    else:
        return struct.pack('!BBH', cmd, 2, mid)

def gen_puback(mid, proto_ver=4, reason_code=-1, properties=None):
    return _gen_command_with_mid(64, mid, proto_ver, reason_code, properties)

def gen_pubrec(mid, proto_ver=4, reason_code=-1, properties=None):
    return _gen_command_with_mid(80, mid, proto_ver, reason_code, properties)

def gen_pubrel(mid, dup=False, proto_ver=4, reason_code=-1, properties=None):
    if dup:
        cmd = 96+8+2
    else:
        cmd = 96+2
    return _gen_command_with_mid(cmd, mid, proto_ver, reason_code, properties)

def gen_pubcomp(mid, proto_ver=4, reason_code=-1, properties=None):
    return _gen_command_with_mid(112, mid, proto_ver, reason_code, properties)


def gen_subscribe(mid, topic, qos, cmd=130, proto_ver=4, properties=b""):
    topic = topic.encode("utf-8")
    packet = struct.pack("!B", cmd)
    if proto_ver == 5:
        if properties == b"":
            packet += pack_remaining_length(2+1+2+len(topic)+1)
            pack_format = "!HBH"+str(len(topic))+"sB"
            return packet + struct.pack(pack_format, mid, 0, len(topic), topic, qos)
        else:
            properties = mqtt5_props.prop_finalise(properties)
            packet += pack_remaining_length(2+1+2+len(topic)+len(properties))
            pack_format = "!H"+str(len(properties))+"s"+"H"+str(len(topic))+"sB"
            return packet + struct.pack(pack_format, mid, properties, len(topic), topic, qos)
    else:
        packet += pack_remaining_length(2+2+len(topic)+1)
        pack_format = "!HH"+str(len(topic))+"sB"
        return packet + struct.pack(pack_format, mid, len(topic), topic, qos)


def gen_suback(mid, qos, proto_ver=4):
    if proto_ver == 5:
        return struct.pack('!BBHBB', 144, 2+1+1, mid, 0, qos)
    else:
        return struct.pack('!BBHB', 144, 2+1, mid, qos)

def gen_unsubscribe(mid, topic, cmd=162, proto_ver=4, properties=b""):
    topic = topic.encode("utf-8")
    if proto_ver == 5:
        if properties == b"":
            pack_format = "!BBHBH"+str(len(topic))+"s"
            return struct.pack(pack_format, cmd, 2+2+len(topic)+1, mid, 0, len(topic), topic)
        else:
            properties = mqtt5_props.prop_finalise(properties)
            packet = struct.pack("!B", cmd)
            l = 2+2+len(topic)+1+len(properties)  # noqa: E741
            packet += pack_remaining_length(l)
            pack_format = "!HB"+str(len(properties))+"sH"+str(len(topic))+"s"
            packet += struct.pack(pack_format, mid, len(properties), properties, len(topic), topic)
            return packet
    else:
        pack_format = "!BBHH"+str(len(topic))+"s"
        return struct.pack(pack_format, cmd, 2+2+len(topic), mid, len(topic), topic)

def gen_unsubscribe_multiple(mid, topics, proto_ver=4):
    packet = b""
    remaining_length = 0
    for t in topics:
        t = t.encode("utf-8")
        remaining_length += 2+len(t)
        packet += struct.pack("!H"+str(len(t))+"s", len(t), t)

    if proto_ver == 5:
        remaining_length += 2+1

        return struct.pack("!BBHB", 162, remaining_length, mid, 0) + packet
    else:
        remaining_length += 2

        return struct.pack("!BBH", 162, remaining_length, mid) + packet

def gen_unsuback(mid, reason_code=0, proto_ver=4):
    if proto_ver == 5:
        if isinstance(reason_code, list):
            reason_code_count = len(reason_code)
            p = struct.pack('!BBHB', 176, 3+reason_code_count, mid, 0)
            for r in reason_code:
                p += struct.pack('B', r)
            return p
        else:
            return struct.pack('!BBHBB', 176, 4, mid, 0, reason_code)
    else:
        return struct.pack('!BBH', 176, 2, mid)

def gen_pingreq():
    return struct.pack('!BB', 192, 0)

def gen_pingresp():
    return struct.pack('!BB', 208, 0)


def _gen_short(cmd, reason_code=-1, proto_ver=5, properties=None):
    if proto_ver == 5 and (reason_code != -1 or properties is not None):
        if reason_code == -1:
             reason_code = 0

        if properties is None:
            return struct.pack('!BBB', cmd, 1, reason_code)
        elif properties == "":
            return struct.pack('!BBBB', cmd, 2, reason_code, 0)
        else:
            properties = mqtt5_props.prop_finalise(properties)
            return struct.pack("!BBB", cmd, 1+len(properties), reason_code) + properties
    else:
        return struct.pack('!BB', cmd, 0)

def gen_disconnect(reason_code=-1, proto_ver=4, properties=None):
    return _gen_short(0xE0, reason_code, proto_ver, properties)

def gen_auth(reason_code=-1, properties=None):
    return _gen_short(0xF0, reason_code, 5, properties)


def pack_remaining_length(remaining_length):
    s = b""
    while True:
        byte = remaining_length % 128
        remaining_length = remaining_length // 128
        # If there are more digits to encode, set the top bit of this digit
        if remaining_length > 0:
            byte = byte | 0x80

        s = s + struct.pack("!B", byte)
        if remaining_length == 0:
            return s


def signal_client_ready():
    ready_file = os.environ.get("PAHO_TEST_READY_FILE")
    if ready_file is not None:
        with open(ready_file, "w"):
            pass


def loop_until_keyboard_interrupt(mqttc):
    """
    Call loop() in a loop until KeyboardInterrupt is received.

    This is used by the test clients in `lib/clients`;
    the client spawner will send a SIGINT to the client process
    when it wants the client to stop, so we should catch that
    and stop the client gracefully.
    """
    try:
        signal_client_ready()
        while True:
            mqttc.loop()
    except KeyboardInterrupt:
        pass


@contextlib.contextmanager
def wait_for_keyboard_interrupt():
    """
    Run the code in the context manager, then wait for a KeyboardInterrupt.

    This is used by the test clients in `lib/clients`;
    the client spawner will send a SIGINT to the client process
    when it wants the client to stop, so we should catch that
    and stop the client gracefully.
    """
    yield  # If we get a KeyboardInterrupt during the block, it's too soon!
    try:
        signal_client_ready()
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        pass


def get_test_server_port() -> int:
    """
    Get the port number for the test server.
    """
    return int(os.environ['PAHO_SERVER_PORT'])
