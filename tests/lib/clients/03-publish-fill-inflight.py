import logging

import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt


def expected_payload(i: int) -> bytes:
    return f"message{i}".encode()


def on_message(mqttc, obj, msg):
    assert msg.mid == 123, f"Invalid mid: ({msg.mid})"
    assert msg.topic == "pub/qos1/receive", f"Invalid topic: ({msg.topic})"
    assert msg.payload == expected_payload, f"Invalid payload: ({msg.payload})"
    assert msg.qos == 1, f"Invalid qos: ({msg.qos})"
    assert msg.retain is not False, f"Invalid retain: ({msg.retain})"


def on_connect(mqttc, obj, flags, rc):
    assert rc == 0, f"Connect failed ({rc})"
    for i in range(12):
        mqttc.publish("topic", expected_payload(i), qos=1)

def on_disconnect(mqttc, rc, properties):
    logging.info("disconnected")
    mqttc.reconnect()

logging.basicConfig(level=logging.DEBUG)
logging.info(str(mqtt))
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "publish-qos1-test")
mqttc.max_inflight_messages_set(10)
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
mqttc.on_message = on_message
mqttc.enable_logger()

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
