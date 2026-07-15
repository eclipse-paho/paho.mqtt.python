import binascii
import struct


def dump_packet(prefix: str, data: bytes) -> None:
    try:
        data = to_string(data)
        print(prefix, ": ", data, sep="")
    except struct.error:
        data = binascii.b2a_hex(data).decode('utf8')
        print(prefix, " (not decoded): 0x", data, sep="")


def remaining_length(packet: bytes) -> tuple[bytes, int]:
    l = min(5, len(packet))  # noqa: E741
    all_bytes = struct.unpack("!" + "B" * l, packet[:l])
    mult = 1
    rl = 0
    for i in range(1, l - 1):
        byte = all_bytes[i]

        rl += (byte & 127) * mult
        mult *= 128
        if byte & 128 == 0:
            packet = packet[i + 1:]
            break

    return (packet, rl)


def to_hex_string(packet: bytes) -> str:
    if not packet:
        return ""

    s = ""
    while len(packet) > 0:
        packet0 = struct.unpack("!B", packet[0])
        s = s+hex(packet0[0]) + " "
        packet = packet[1:]

    return s


def to_string(packet: bytes) -> str:
    if not packet:
        return ""

    packet0 = struct.unpack("!B%ds" % (len(packet)-1), bytes(packet))
    packet0 = packet0[0]
    cmd = packet0 & 0xF0
    if cmd == 0x00:
        # Reserved
        return "0x00"
    elif cmd == 0x10:
        # CONNECT
        (packet, rl) = remaining_length(packet)
        pack_format = "!H" + str(len(packet) - 2) + 's'
        (slen, packet) = struct.unpack(pack_format, packet)
        pack_format = "!" + str(slen) + 'sBBH' + str(len(packet) - slen - 4) + 's'
        (protocol, proto_ver, flags, keepalive, packet) = struct.unpack(pack_format, packet)
        kind = ("clean-session" if flags & 2 else "durable")
        s = f"CONNECT, proto={protocol}{proto_ver}, keepalive={keepalive}, {kind}"

        pack_format = "!H" + str(len(packet) - 2) + 's'
        (slen, packet) = struct.unpack(pack_format, packet)
        pack_format = "!" + str(slen) + 's' + str(len(packet) - slen) + 's'
        (client_id, packet) = struct.unpack(pack_format, packet)
        s = s + ", id=" + str(client_id)

        if flags & 4:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (slen, packet) = struct.unpack(pack_format, packet)
            pack_format = "!" + str(slen) + 's' + str(len(packet) - slen) + 's'
            (will_topic, packet) = struct.unpack(pack_format, packet)
            s = s + ", will-topic=" + str(will_topic)

            pack_format = "!H" + str(len(packet) - 2) + 's'
            (slen, packet) = struct.unpack(pack_format, packet)
            pack_format = "!" + str(slen) + 's' + str(len(packet) - slen) + 's'
            (will_message, packet) = struct.unpack(pack_format, packet)
            s = s + ", will-message=" + will_message

            s = s + ", will-qos=" + str((flags & 24) >> 3)
            s = s + ", will-retain=" + str((flags & 32) >> 5)

        if flags & 128:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (slen, packet) = struct.unpack(pack_format, packet)
            pack_format = "!" + str(slen) + 's' + str(len(packet) - slen) + 's'
            (username, packet) = struct.unpack(pack_format, packet)
            s = s + ", username=" + str(username)

        if flags & 64:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (slen, packet) = struct.unpack(pack_format, packet)
            pack_format = "!" + str(slen) + 's' + str(len(packet) - slen) + 's'
            (password, packet) = struct.unpack(pack_format, packet)
            s = s + ", password=" + str(password)

        if flags & 1:
            s = s + ", reserved=1"

        return s
    elif cmd == 0x20:
        # CONNACK
        if len(packet) == 4:
            (cmd, rl, resv, rc) = struct.unpack('!BBBB', packet)
            return "CONNACK, rl="+str(rl)+", res="+str(resv)+", rc="+str(rc)
        elif len(packet) == 5:
            (cmd, rl, flags, reason_code, proplen) = struct.unpack('!BBBBB', packet)
            return "CONNACK, rl="+str(rl)+", flags="+str(flags)+", rc="+str(reason_code)+", proplen="+str(proplen)
        else:
            return "CONNACK, (not decoded)"

    elif cmd == 0x30:
        # PUBLISH
        dup = (packet0 & 0x08) >> 3
        qos = (packet0 & 0x06) >> 1
        retain = (packet0 & 0x01)
        (packet, rl) = remaining_length(packet)
        pack_format = "!H" + str(len(packet) - 2) + 's'
        (tlen, packet) = struct.unpack(pack_format, packet)
        pack_format = "!" + str(tlen) + 's' + str(len(packet) - tlen) + 's'
        (topic, packet) = struct.unpack(pack_format, packet)
        s = "PUBLISH, rl=" + str(rl) + ", topic=" + str(topic) + ", qos=" + str(qos) + ", retain=" + str(retain) + ", dup=" + str(dup)
        if qos > 0:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (mid, packet) = struct.unpack(pack_format, packet)
            s = s + ", mid=" + str(mid)

        s = s + ", payload=" + str(packet)
        return s
    elif cmd == 0x40:
        # PUBACK
        if len(packet) == 5:
            (cmd, rl, mid, reason_code) = struct.unpack('!BBHB', packet)
            return "PUBACK, rl="+str(rl)+", mid="+str(mid)+", reason_code="+str(reason_code)
        else:
            (cmd, rl, mid) = struct.unpack('!BBH', packet)
            return "PUBACK, rl="+str(rl)+", mid="+str(mid)
    elif cmd == 0x50:
        # PUBREC
        if len(packet) == 5:
            (cmd, rl, mid, reason_code) = struct.unpack('!BBHB', packet)
            return "PUBREC, rl="+str(rl)+", mid="+str(mid)+", reason_code="+str(reason_code)
        else:
            (cmd, rl, mid) = struct.unpack('!BBH', packet)
            return "PUBREC, rl="+str(rl)+", mid="+str(mid)
    elif cmd == 0x60:
        # PUBREL
        dup = (packet0 & 0x08) >> 3
        (cmd, rl, mid) = struct.unpack('!BBH', packet)
        return "PUBREL, rl=" + str(rl) + ", mid=" + str(mid) + ", dup=" + str(dup)
    elif cmd == 0x70:
        # PUBCOMP
        (cmd, rl, mid) = struct.unpack('!BBH', packet)
        return "PUBCOMP, rl=" + str(rl) + ", mid=" + str(mid)
    elif cmd == 0x80:
        # SUBSCRIBE
        (packet, rl) = remaining_length(packet)
        pack_format = "!H" + str(len(packet) - 2) + 's'
        (mid, packet) = struct.unpack(pack_format, packet)
        s = "SUBSCRIBE, rl=" + str(rl) + ", mid=" + str(mid)
        topic_index = 0
        while len(packet) > 0:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (tlen, packet) = struct.unpack(pack_format, packet)
            pack_format = "!" + str(tlen) + 'sB' + str(len(packet) - tlen - 1) + 's'
            (topic, qos, packet) = struct.unpack(pack_format, packet)
            s = s + ", topic" + str(topic_index) + "=" + str(topic) + "," + str(qos)
        return s
    elif cmd == 0x90:
        # SUBACK
        (packet, rl) = remaining_length(packet)
        pack_format = "!H" + str(len(packet) - 2) + 's'
        (mid, packet) = struct.unpack(pack_format, packet)
        pack_format = "!" + "B" * len(packet)
        granted_qos = struct.unpack(pack_format, packet)

        s = "SUBACK, rl=" + str(rl) + ", mid=" + str(mid) + ", granted_qos=" + str(granted_qos[0])
        for i in range(1, len(granted_qos) - 1):
            s = s + ", " + str(granted_qos[i])
        return s
    elif cmd == 0xA0:
        # UNSUBSCRIBE
        (packet, rl) = remaining_length(packet)
        pack_format = "!H" + str(len(packet) - 2) + 's'
        (mid, packet) = struct.unpack(pack_format, packet)
        s = "UNSUBSCRIBE, rl=" + str(rl) + ", mid=" + str(mid)
        topic_index = 0
        while len(packet) > 0:
            pack_format = "!H" + str(len(packet) - 2) + 's'
            (tlen, packet) = struct.unpack(pack_format, packet)
            pack_format = "!" + str(tlen) + 's' + str(len(packet) - tlen) + 's'
            (topic, packet) = struct.unpack(pack_format, packet)
            s = s + ", topic" + str(topic_index) + "=" + str(topic)
        return s
    elif cmd == 0xB0:
        # UNSUBACK
        (cmd, rl, mid) = struct.unpack('!BBH', packet)
        return "UNSUBACK, rl=" + str(rl) + ", mid=" + str(mid)
    elif cmd == 0xC0:
        # PINGREQ
        (cmd, rl) = struct.unpack('!BB', packet)
        return "PINGREQ, rl=" + str(rl)
    elif cmd == 0xD0:
        # PINGRESP
        (cmd, rl) = struct.unpack('!BB', packet)
        return "PINGRESP, rl=" + str(rl)
    elif cmd == 0xE0:
        # DISCONNECT
        if len(packet) == 3:
            (cmd, rl, reason_code) = struct.unpack('!BBB', packet)
            return "DISCONNECT, rl="+str(rl)+", reason_code="+str(reason_code)
        else:
            (cmd, rl) = struct.unpack('!BB', packet)
            return "DISCONNECT, rl="+str(rl)
    elif cmd == 0xF0:
        # AUTH
        (cmd, rl) = struct.unpack('!BB', packet)
        return "AUTH, rl="+str(rl)
    raise ValueError(f"Unknown packet type {cmd}")
