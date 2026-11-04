import logging

import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

expected_payload = b"message"


def on_message(mqttc, obj, msg):
    assert msg.mid == 13423, f"Invalid mid: ({msg.mid})"
    assert msg.topic == "pub/qos2/receive", f"Invalid topic: ({msg.topic})"
    assert msg.payload == expected_payload, f"Invalid payload: ({msg.payload})"
    assert msg.qos == 2, f"Invalid qos: ({msg.qos})"
    assert not msg.retain, f"Invalid retain: ({msg.retain})"


def on_connect(mqttc, obj, flags, rc):
    assert rc == 0, f"Connect failed ({rc})"

logging.basicConfig(level=logging.DEBUG)
mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "publish-qos2-test", clean_session=True)
mqttc.enable_logger()
mqttc.on_connect = on_connect
mqttc.on_message = on_message

mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
