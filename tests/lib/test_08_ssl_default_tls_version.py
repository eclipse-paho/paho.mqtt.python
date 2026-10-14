import ssl

import paho.mqtt.client as mqtt


def test_08_ssl_default_tls_version():
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "08-ssl-default-tls-version")
    mqttc.tls_set()
    assert mqttc._ssl_context.protocol == ssl.PROTOCOL_TLS_CLIENT


def test_08_ssl_default_tls_version_without_tlsv1_2_constant(monkeypatch):
    # Python built against OpenSSL >= 4.0 does not define the
    # version-specific PROTOCOL_TLSv1* constants.
    monkeypatch.delattr(ssl, "PROTOCOL_TLSv1_2", raising=False)
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "08-ssl-default-tls-version")
    mqttc.tls_set()
    assert mqttc._ssl_context.protocol == ssl.PROTOCOL_TLS_CLIENT
