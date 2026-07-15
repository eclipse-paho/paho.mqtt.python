"""Fake sockets and helpers used by brokerless benchmark scenarios."""

from __future__ import absolute_import

import collections


class FakeRecvSocket(object):
    def __init__(self, data):
        self._data = bytearray(data)
        self._pos = 0
        self.closed = False

    def recv(self, size):
        if self._pos >= len(self._data):
            return b""
        end = min(self._pos + size, len(self._data))
        chunk = bytes(self._data[self._pos:end])
        self._pos = end
        return chunk

    def send(self, data):
        return len(data)

    def close(self):
        self.closed = True

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


class NonBlockingRecvSocket(FakeRecvSocket):
    """Burst socket that raises EAGAIN after its current input is drained."""

    def __init__(self, data):
        super(NonBlockingRecvSocket, self).__init__(data)
        self.recv_calls = 0

    def recv(self, size):
        self.recv_calls += 1
        if self._pos >= len(self._data):
            raise BlockingIOError()
        return super(NonBlockingRecvSocket, self).recv(size)


class FakeSendSocket(object):
    def __init__(self):
        self.bytes_sent = 0
        self.calls = 0
        self.closed = False

    def recv(self, size):
        return b""

    def send(self, data):
        length = len(data)
        self.bytes_sent += length
        self.calls += 1
        return length

    def close(self):
        self.closed = True

    def fileno(self):
        return 1

    def setblocking(self, flag):
        return None


def make_out_packet(command, packet, mid=1, qos=0, info=None):
    return {
        "command": command,
        "mid": mid,
        "qos": qos,
        "pos": 0,
        "to_process": len(packet),
        "packet": packet,
        "info": info,
    }


def packet_deque(packets):
    return collections.deque(packets)
