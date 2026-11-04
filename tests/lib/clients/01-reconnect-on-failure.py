import sys

import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, wait_for_keyboard_interrupt


def on_connect(mqttc, obj, flags, rc):
    mqttc.publish("reconnect/test", "message")


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "01-reconnect-on-failure", reconnect_on_failure=False)
mqttc.on_connect = on_connect

with wait_for_keyboard_interrupt():
    mqttc.connect("localhost", get_test_server_port())
    mqttc.loop_forever()
    sys.exit(42)  # this is expected by the test case
