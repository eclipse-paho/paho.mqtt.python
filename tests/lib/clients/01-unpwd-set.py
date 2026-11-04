import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "01-unpwd-set")

mqttc.username_pw_set("uname", ";'[08gn=#")
mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
