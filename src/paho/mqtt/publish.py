# Copyright (c) 2014 Roger Light <roger@atchoo.org>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v2.0
# and Eclipse Distribution License v1.0 which accompany this distribution.
#
# The Eclipse Public License is available at
#    http://www.eclipse.org/legal/epl-v20.html
# and the Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    Roger Light - initial API and implementation

"""
This module provides some helper functions to allow straightforward publishing
of messages in a one-shot manner. In other words, they are useful for the
situation where you have a single/multiple messages you want to publish to a
broker, then disconnect and nothing else is required.
"""
from __future__ import annotations

import collections
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Union

from paho.mqtt.enums import CallbackAPIVersion, MQTTProtocolVersion
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from .. import mqtt
from . import client as paho

_PUBLISH_WINDOW = 20

if TYPE_CHECKING:
    from typing import Literal, TypedDict

    try:
        from typing import NotRequired, Required  # type: ignore
    except ImportError:
        from typing_extensions import NotRequired, Required

    class AuthParameter(TypedDict, total=False):
        username: Required[str]
        password: NotRequired[str]


    class TLSParameter(TypedDict, total=False):
        ca_certs: Required[str]
        certfile: NotRequired[str]
        keyfile: NotRequired[str]
        tls_version: NotRequired[int]
        ciphers: NotRequired[str]
        insecure: NotRequired[bool]


    class MessageDict(TypedDict, total=False):
        topic: Required[str]
        payload: NotRequired[paho.PayloadType]
        qos: NotRequired[int]
        retain: NotRequired[bool]

    MessageTuple = tuple[str, paho.PayloadType, int, bool]

    MessagesList = list[Union[MessageDict, MessageTuple]]


def _publish_message(client: paho.Client, message):
    """Publish one validated helper input and return its info and QoS."""
    if isinstance(message, dict):
        return client.publish(**message), message.get("qos", 0)
    elif isinstance(message, (tuple, list)):
        return client.publish(*message), message[2] if len(message) > 2 else 0
    else:
        raise TypeError('message must be a dict, tuple, or list')


class _MultipleState:
    """Bounded submission/completion state for :func:`multiple`."""

    __slots__ = (
        "messages",
        "outstanding",
        "pending_mids",
        "early_completed_mids",
        "filling",
        "disconnecting",
        "error",
    )

    def __init__(self, messages):
        self.messages = collections.deque(messages)
        self.outstanding = 0
        self.pending_mids = set()
        self.early_completed_mids = set()
        self.filling = False
        self.disconnecting = False
        self.error = None


def _set_error(state: _MultipleState, error: Exception) -> None:
    if state.error is None:
        state.error = error


def _disconnect_if_finished(client: paho.Client, state: _MultipleState) -> None:
    source_finished = not state.messages or state.error is not None
    if source_finished and state.outstanding == 0 and not state.disconnecting:
        state.disconnecting = True
        client.disconnect()


def _fill_window(client: paho.Client, state: _MultipleState) -> None:
    if state.filling or state.disconnecting or state.error is not None:
        _disconnect_if_finished(client, state)
        return

    state.filling = True
    try:
        while (
            state.messages
            and state.outstanding < _PUBLISH_WINDOW
            and state.error is None
        ):
            message = state.messages.popleft()
            # Reserve before publish() so a synchronous fake/custom client
            # callback cannot make the count negative before the MID returns.
            state.outstanding += 1
            try:
                info, qos = _publish_message(client, message)
            except Exception as error:
                if state.outstanding > len(state.pending_mids):
                    state.outstanding -= 1
                _set_error(state, error)
                break

            completed_early = info.mid in state.early_completed_mids
            if completed_early:
                state.early_completed_mids.remove(info.mid)

            accepted_while_disconnected = (
                qos > 0 and info.rc == paho.MQTT_ERR_NO_CONN
            )
            if info.rc != paho.MQTT_ERR_SUCCESS and not accepted_while_disconnected:
                if not completed_early:
                    state.outstanding -= 1
                _set_error(state, mqtt.MQTTException(paho.error_string(info.rc)))
                break

            if completed_early:
                continue
            if info.mid in state.pending_mids:
                state.outstanding -= 1
                _set_error(
                    state,
                    mqtt.MQTTException("Message identifier collision"),
                )
                break
            state.pending_mids.add(info.mid)
    finally:
        state.filling = False

    _disconnect_if_finished(client, state)


def _on_connect(client: paho.Client, userdata: _MultipleState, flags, reason_code, properties):
    """Internal v5 callback"""
    if reason_code == 0:
        _fill_window(client, userdata)
    else:
        _set_error(
            userdata,
            mqtt.MQTTException(paho.connack_string(reason_code)),
        )
        _disconnect_if_finished(client, userdata)


def _on_publish(
    client: paho.Client, userdata: _MultipleState, mid: int, reason_codes: ReasonCode, properties: Properties,
) -> None:
    """Internal callback"""
    #pylint: disable=unused-argument

    if mid in userdata.pending_mids:
        userdata.pending_mids.remove(mid)
        userdata.outstanding -= 1
    elif (
        userdata.filling
        and userdata.outstanding > len(userdata.pending_mids)
    ):
        # A custom/fake client may complete synchronously before publish()
        # returns its MID. Real Client callbacks are serialized, but keeping
        # this state machine reentrant costs nothing on the normal path.
        if mid in userdata.early_completed_mids:
            return
        userdata.early_completed_mids.add(mid)
        userdata.outstanding -= 1
    else:
        # Client normally suppresses duplicate completions. Ignore an unknown
        # callback defensively rather than releasing a slot twice.
        return

    _fill_window(client, userdata)


def multiple(
    msgs: MessagesList,
    hostname: str = "localhost",
    port: int = 1883,
    client_id: str = "",
    keepalive: int = 60,
    will: MessageDict | None = None,
    auth: AuthParameter | None = None,
    tls: TLSParameter | None = None,
    protocol: MQTTProtocolVersion = paho.MQTTv311,
    transport: Literal["tcp", "websockets"] = "tcp",
    proxy_args: Any | None = None,
) -> None:
    """Publish multiple messages to a broker, then disconnect cleanly.

    This function creates an MQTT client, connects to a broker and publishes a
    list of messages. Once the messages have been delivered, it disconnects
    cleanly from the broker.

    :param msgs: a list of messages to publish. Each message is either a dict or a
           tuple.

           If a dict, only the topic must be present. Default values will be
           used for any missing arguments. The dict must be of the form:

           msg = {'topic':"<topic>", 'payload':"<payload>", 'qos':<qos>,
           'retain':<retain>}
           topic must be present and may not be empty.
           If payload is "", None or not present then a zero length payload
           will be published.
           If qos is not present, the default of 0 is used.
           If retain is not present, the default of False is used.

           If a tuple, then it must be of the form:
           ("<topic>", "<payload>", qos, retain)

    :param str hostname: the address of the broker to connect to.
               Defaults to localhost.

    :param int port: the port to connect to the broker on. Defaults to 1883.

    :param str client_id: the MQTT client id to use. If "" or None, the Paho library will
                generate a client id automatically.

    :param int keepalive: the keepalive timeout value for the client. Defaults to 60
                seconds.

    :param will: a dict containing will parameters for the client: will = {'topic':
           "<topic>", 'payload':"<payload">, 'qos':<qos>, 'retain':<retain>}.
           Topic is required, all other parameters are optional and will
           default to None, 0 and False respectively.
           Defaults to None, which indicates no will should be used.

    :param auth: a dict containing authentication parameters for the client:
           auth = {'username':"<username>", 'password':"<password>"}
           Username is required, password is optional and will default to None
           if not provided.
           Defaults to None, which indicates no authentication is to be used.

    :param tls: a dict containing TLS configuration parameters for the client:
          dict = {'ca_certs':"<ca_certs>", 'certfile':"<certfile>",
          'keyfile':"<keyfile>", 'tls_version':"<tls_version>",
          'ciphers':"<ciphers">, 'insecure':"<bool>"}
          ca_certs is required, all other parameters are optional and will
          default to None if not provided, which results in the client using
          the default behaviour - see the paho.mqtt.client documentation.
          Alternatively, tls input can be an SSLContext object, which will be
          processed using the tls_set_context method.
          Defaults to None, which indicates that TLS should not be used.

    :param str transport: set to "tcp" to use the default setting of transport which is
          raw TCP. Set to "websockets" to use WebSockets as the transport.

    :param proxy_args: a dictionary that will be given to the client.
    """

    if not isinstance(msgs, Iterable):
        raise TypeError('msgs must be an iterable')
    if len(msgs) == 0:
        raise ValueError('msgs is empty')

    state = _MultipleState(msgs)
    client = paho.Client(
        CallbackAPIVersion.VERSION2,
        client_id=client_id,
        userdata=state,
        protocol=protocol,
        transport=transport,
    )

    client.enable_logger()
    client.on_publish = _on_publish
    client.on_connect = _on_connect  # type: ignore

    if proxy_args is not None:
        client.proxy_set(**proxy_args)

    if auth:
        username = auth.get('username')
        if username:
            password = auth.get('password')
            client.username_pw_set(username, password)
        else:
            raise KeyError("The 'username' key was not found, this is "
                           "required for auth")

    if will is not None:
        client.will_set(**will)

    if tls is not None:
        if isinstance(tls, dict):
            insecure = tls.pop('insecure', False)
            # mypy don't get that tls no longer contains the key insecure
            client.tls_set(**tls)  # type: ignore[misc]
            if insecure:
                # Must be set *after* the `client.tls_set()` call since it sets
                # up the SSL context that `client.tls_insecure_set` alters.
                client.tls_insecure_set(insecure)
        else:
            # Assume input is SSLContext object
            client.tls_set_context(tls)

    client.connect(hostname, port, keepalive)
    client.loop_forever()
    if state.error is not None:
        raise state.error


def single(
    topic: str,
    payload: paho.PayloadType = None,
    qos: int = 0,
    retain: bool = False,
    hostname: str = "localhost",
    port: int = 1883,
    client_id: str = "",
    keepalive: int = 60,
    will: MessageDict | None = None,
    auth: AuthParameter | None = None,
    tls: TLSParameter | None = None,
    protocol: MQTTProtocolVersion = paho.MQTTv311,
    transport: Literal["tcp", "websockets"] = "tcp",
    proxy_args: Any | None = None,
) -> None:
    """Publish a single message to a broker, then disconnect cleanly.

    This function creates an MQTT client, connects to a broker and publishes a
    single message. Once the message has been delivered, it disconnects cleanly
    from the broker.

    :param str topic: the only required argument must be the topic string to which the
            payload will be published.

    :param payload: the payload to be published. If "" or None, a zero length payload
              will be published.

    :param int qos: the qos to use when publishing,  default to 0.

    :param bool retain: set the message to be retained (True) or not (False).

    :param str hostname: the address of the broker to connect to.
               Defaults to localhost.

    :param int port: the port to connect to the broker on. Defaults to 1883.

    :param str client_id: the MQTT client id to use. If "" or None, the Paho library will
                generate a client id automatically.

    :param int keepalive: the keepalive timeout value for the client. Defaults to 60
                seconds.

    :param will: a dict containing will parameters for the client: will = {'topic':
           "<topic>", 'payload':"<payload">, 'qos':<qos>, 'retain':<retain>}.
           Topic is required, all other parameters are optional and will
           default to None, 0 and False respectively.
           Defaults to None, which indicates no will should be used.

    :param auth: a dict containing authentication parameters for the client:
           Username is required, password is optional and will default to None
           auth = {'username':"<username>", 'password':"<password>"}
           if not provided.
           Defaults to None, which indicates no authentication is to be used.

    :param tls: a dict containing TLS configuration parameters for the client:
          dict = {'ca_certs':"<ca_certs>", 'certfile':"<certfile>",
          'keyfile':"<keyfile>", 'tls_version':"<tls_version>",
          'ciphers':"<ciphers">, 'insecure':"<bool>"}
          ca_certs is required, all other parameters are optional and will
          default to None if not provided, which results in the client using
          the default behaviour - see the paho.mqtt.client documentation.
          Defaults to None, which indicates that TLS should not be used.
          Alternatively, tls input can be an SSLContext object, which will be
          processed using the tls_set_context method.

    :param transport: set to "tcp" to use the default setting of transport which is
          raw TCP. Set to "websockets" to use WebSockets as the transport.

    :param proxy_args: a dictionary that will be given to the client.
    """

    msg: MessageDict = {'topic':topic, 'payload':payload, 'qos':qos, 'retain':retain}

    multiple([msg], hostname, port, client_id, keepalive, will, auth, tls,
             protocol, transport, proxy_args)
