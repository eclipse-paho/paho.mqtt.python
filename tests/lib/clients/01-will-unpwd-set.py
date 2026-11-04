import paho.mqtt.client as mqtt
from tests.paho_test import get_test_server_port, loop_until_keyboard_interrupt

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "01-will-unpwd-set")

mqttc.username_pw_set("oibvvwqw", "#'^2hg9a&nm38*us")
mqttc.will_set("will-topic", "will message", 2, False)
mqttc.connect("localhost", get_test_server_port())
loop_until_keyboard_interrupt(mqttc)
