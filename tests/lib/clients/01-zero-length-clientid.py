import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt


def on_connect(mqttc, obj, flags, rc):
    assert rc == 0, f"Connect failed ({rc})"
    mqttc.disconnect()


def on_disconnect(mqttc, obj, rc):
    mqttc.loop()


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "", clean_session=True, protocol=mqtt.MQTTv311)
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
