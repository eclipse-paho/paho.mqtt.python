import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

sent_mid = -1


def on_connect(mqttc, obj, flags, rc):
    global sent_mid
    assert rc == 0, f"Connect failed ({rc})"
    (_res, sent_mid) = mqttc.publish("pub/qos0/no-payload/test")


def on_publish(mqttc, obj, mid):
    if sent_mid == mid:
        mqttc.disconnect()


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "publish-qos0-test-np", clean_session=True)
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
