import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt


def on_connect(mqttc, obj, flags, rc):
    assert rc == 0, f"Connect failed ({rc})"
    mqttc.unsubscribe("unsubscribe/test")


def on_unsubscribe(mqttc, obj, mid):
    mqttc.disconnect()


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "unsubscribe-test", clean_session=True)
mqttc.on_connect = on_connect
mqttc.on_unsubscribe = on_unsubscribe

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
