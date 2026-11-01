import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "decorators-test", clean_session=True)
payload = b""


@mqttc.connect_callback()
def on_connect(mqttc, obj, flags, rc):
    mqttc.subscribe("sub-test", 1)


@mqttc.subscribe_callback()
def on_subscribe(mqttc, obj, mid, granted_qos):
    mqttc.unsubscribe("unsub-test")


@mqttc.unsubscribe_callback()
def on_unsubscribe(mqttc, obj, mid):
    global payload
    payload = "message"


@mqttc.message_callback()
def on_message(mqttc, obj, msg):
    mqttc.publish("decorators", qos=1, payload=payload)


@mqttc.publish_callback()
def on_publish(mqttc, obj, mid):
    mqttc.disconnect()


@mqttc.disconnect_callback()
def on_disconnect(mqttc, obj, rc):
    pass  # TODO: should probably test that this gets called


mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
