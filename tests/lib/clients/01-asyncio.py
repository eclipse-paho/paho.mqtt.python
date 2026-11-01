import asyncio
import socket

import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port

client_id = 'asyncio-test'


class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):
        def cb():
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):
        def cb():
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break


async def main():
    loop = asyncio.get_event_loop()
    payload = ""

    def on_connect(client, obj, flags, rc):
        client.subscribe("sub-test", 1)

    def on_subscribe(client, obj, mid, granted_qos):
        client.unsubscribe("unsub-test")

    def on_unsubscribe(client, obj, mid):
        nonlocal payload
        payload = "message"

    def on_message(client, obj, msg):
        client.publish("asyncio", qos=1, payload=payload)

    def on_publish(client, obj, mid):
        client.disconnect()

    def on_disconnect(client, userdata, rc):
        disconnected.set_result(rc)

    disconnected = loop.create_future()

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1, client_id=client_id)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe
    client.on_unsubscribe = on_unsubscribe
    client.on_disconnect = on_disconnect

    _aioh = AsyncioHelper(loop, client)

    client.connect('localhost', get_test_server_port(), 60)
    client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)

    await disconnected

if __name__ == '__main__':
    asyncio.run(main())
