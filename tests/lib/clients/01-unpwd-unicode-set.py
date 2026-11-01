
import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "01-unpwd-unicode-set")

username = "\u00fas\u00e9rn\u00e1m\u00e9-h\u00e9ll\u00f3"
password = "h\u00e9ll\u00f3"
mqttc.username_pw_set(username, password)
mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
