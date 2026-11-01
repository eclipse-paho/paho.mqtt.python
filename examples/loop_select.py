#!/usr/bin/env python3

import socket
import uuid
from select import select
from time import time

import paho.mqtt.client as mqtt

client_id = 'paho-mqtt-python/issue72/' + str(uuid.uuid4())
topic = client_id
print("Using client_id / topic: " + client_id)


class SelectMqttExample:
    def __init__(self):
        pass

    def on_connect(self, client, userdata, flags, reason_code, properties):
        print("Subscribing")
        client.subscribe(topic)

    def on_message(self, client, userdata, msg):
        if self.state not in {1, 3, 5}:
            print("Got unexpected message: {}".format(msg.decode()))
            return

        print("Got message with len {}".format(len(msg.payload)))
        self.state += 1
        self.t = time()

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.disconnected = True, reason_code

    def do_select(self):
        sock = self.client.socket()
        if not sock:
            raise Exception("Socket is gone")  # noqa: TRY002

        print("Selecting for reading" + (" and writing" if self.client.want_write() else ""))
        r, w, _e = select(
            [sock],
            [sock] if self.client.want_write() else [],
            [],
            1
        )

        if sock in r:
            print("Socket is readable, calling loop_read")
            self.client.loop_read()

        if sock in w:
            print("Socket is writable, calling loop_write")
            self.client.loop_write()

        self.client.loop_misc()

    def main(self):
        self.disconnected = (False, None)
        self.t = time()
        self.state = 0

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

        self.client.connect('mqtt.eclipseprojects.io', 1883, 60)
        print("Socket opened")
        self.client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

        while not self.disconnected[0]:
            self.do_select()

            if self.state in {0, 2, 4} and time() - self.t >= 5:
                print("Publishing")
                self.client.publish(topic, b'Hello' * 40000)
                self.state += 1

            if self.state == 6:
                self.state += 1
                self.client.disconnect()

        print("Disconnected: {}".format(self.disconnected[1]))


print("Starting")
SelectMqttExample().main()
print("Finished")
