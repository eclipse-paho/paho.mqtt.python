# SPDX-License-Identifier: EPL-2.0 OR BSD-3-Clause
# Generated with assistance from OpenAI Codex (GPT-6).

import ssl
from unittest.mock import Mock, call, patch

import paho.mqtt.publish as publish
import paho.mqtt.subscribe as subscribe
import pytest


@pytest.fixture(params=["single", "multiple", "callback", "simple"])
def helper(request):
    if request.param == "single":
        return lambda tls: publish.single("test/topic", "payload", tls=tls)
    if request.param == "multiple":
        return lambda tls: publish.multiple([{"topic": "test/topic", "payload": "payload"}], tls=tls)
    if request.param == "callback":
        return lambda tls: subscribe.callback(Mock(), "test/topic", tls=tls)
    return lambda tls: subscribe.simple("test/topic", tls=tls)


@pytest.mark.parametrize("insecure", [None, False, True])
def test_tls_options_can_be_reused(helper, insecure):
    tls = {"ca_certs": "ca.pem"}
    if insecure is not None:
        tls["insecure"] = insecure
    original = tls.copy()

    with patch("paho.mqtt.client.Client", autospec=True) as client_class:
        for _ in range(2):
            client = client_class.return_value
            client.reset_mock()
            helper(tls)
            assert tls == original
            client.tls_set.assert_called_once_with(ca_certs="ca.pem")
            if insecure:
                client.tls_insecure_set.assert_called_once_with(True)
                assert client.method_calls.index(call.tls_set(ca_certs="ca.pem")) < client.method_calls.index(call.tls_insecure_set(True))
            else:
                client.tls_insecure_set.assert_not_called()
            client.tls_set_context.assert_not_called()


def test_tls_context_is_forwarded(helper):
    context = ssl.create_default_context()
    with patch("paho.mqtt.client.Client", autospec=True) as client_class:
        helper(context)
        client = client_class.return_value
        client.tls_set_context.assert_called_once_with(context)
        client.tls_set.assert_not_called()
        client.tls_insecure_set.assert_not_called()
