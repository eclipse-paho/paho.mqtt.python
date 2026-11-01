import pytest

import paho.mqtt.client as mqtt


def test_08_ssl_bad_cacert():
    with pytest.raises(IOError):
        mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, "08-ssl-bad-cacert")
        mqttc.tls_set("this/file/doesnt/exist")
