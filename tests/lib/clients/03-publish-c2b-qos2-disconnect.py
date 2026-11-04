import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

first_connection = 1


def on_connect(mqttc, obj, flags, rc):
    global first_connection
    assert rc == 0, f"Connect failed ({rc})"
    if first_connection == 1:
        mqttc.publish("pub/qos2/test", "message", 2)
        first_connection = 0


def on_disconnect(mqttc, obj, rc):
    if rc != 0:
        mqttc.reconnect()


def on_publish(mqttc, obj, mid):
    mqttc.disconnect()


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "publish-qos2-test", clean_session=False)
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
