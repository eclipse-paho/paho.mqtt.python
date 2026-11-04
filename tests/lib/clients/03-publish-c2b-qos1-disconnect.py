import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

sent_mid = -1


def on_connect(mqttc, obj, flags, rc):
    global sent_mid
    assert rc == 0, f"Connect failed ({rc})"
    if sent_mid == -1:
        res = mqttc.publish("pub/qos1/test", "message", 1)
        sent_mid = res[1]


def on_disconnect(mqttc, obj, rc):
    if rc != mqtt.MQTT_ERR_SUCCESS:
        mqttc.reconnect()


def on_publish(mqttc, obj, mid):
    assert mid == sent_mid, f"Invalid mid: ({mid})"
    mqttc.disconnect()


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "publish-qos1-test", clean_session=False)
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_publish = on_publish

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
