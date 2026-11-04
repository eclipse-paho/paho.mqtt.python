import paho.mqtt.client
import paho.mqtt.publish
from tests.paho_test import get_test_server_port, wait_for_keyboard_interrupt

with wait_for_keyboard_interrupt():
    paho.mqtt.publish.single(
        "pub/qos0/test",
        "message",
        qos=0,
        hostname="localhost",
        port=get_test_server_port(),
        client_id="publish-helper-qos0-test",
        protocol=paho.mqtt.client.MQTTv5,
    )
