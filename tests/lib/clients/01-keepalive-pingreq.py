import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt


def on_connect(mqttc, obj, flags, rc):
    assert rc == 0, f"Connect failed ({rc})"


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "01-keepalive-pingreq")
mqttc.on_connect = on_connect

mqttc.connect("localhost", get_test_server_port(), keepalive=4)
loop_until_keyboard_interrupt(mqttc)
