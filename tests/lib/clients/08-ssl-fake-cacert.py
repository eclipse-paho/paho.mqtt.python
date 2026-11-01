import os
import ssl

import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, wait_for_keyboard_interrupt


def on_connect(mqttc, obj, flags, rc):
    raise RuntimeError("Connection should have failed!")


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "08-ssl-fake-cacert")
mqttc.tls_set(
    os.path.join(os.environ["PAHO_SSL_PATH"], "test-fake-root-ca.crt"),
    os.path.join(os.environ["PAHO_SSL_PATH"], "client.crt"),
    os.path.join(os.environ["PAHO_SSL_PATH"], "client.key"),
)
mqttc.on_connect = on_connect

with wait_for_keyboard_interrupt():
    try:
        mqttc.connect("localhost", get_test_server_port())
    except ssl.SSLError as msg:
        assert msg.errno == 1 and "certificate verify failed" in msg.strerror
    else:
        raise Exception("Expected SSLError")
