import os

import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt


def on_connect(mqttc, obj, flags, rc):
    assert rc == 0, f"Connect failed ({rc})"
    mqttc.disconnect()


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "08-ssl-connect-alpn", clean_session=True)
mqttc.tls_set(
    os.path.join(os.environ["PAHO_SSL_PATH"], "all-ca.crt"),
    os.path.join(os.environ["PAHO_SSL_PATH"], "client.crt"),
    os.path.join(os.environ["PAHO_SSL_PATH"], "client.key"),
    alpn_protocols=["paho-test-protocol"],
)
mqttc.on_connect = on_connect

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
