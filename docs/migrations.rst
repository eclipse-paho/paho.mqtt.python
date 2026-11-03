Migrations
==========

Change between version 1.x and 2.0
----------------------------------

Most breaking change should break loudly and should not be missed. The
most significant one which affect everyone is the versioned user callbacks.
Other breaking change might not effect your usage of paho-mqtt.

The list of breaking change (detailed below) are:

* Add version to user callbacks (on_publish, on_connect...).
  tl; dr: add ``mqtt.CallbackAPIVersion.VERSION1`` as first argument to `Client()`
* Drop support for older Python.
* Dropped some deprecated and no used argument or method. If you used them, you can just drop them.
* Removed from public interface few function/class
* Renamed ReasonCodes to ReasonCode
* Improved typing which resulted in few type change. It might no affect you, see below for detail.
* Fixed connect_srv, which changed its signature.
* Added new properties, which could conflict with sub-class

Versioned the user callbacks
****************************

Version 2.0 of paho-mqtt introduced versioning of the user-callback. To fix some inconsistency in callback
arguments and to provide better support for MQTTv5, version 2.0 changed the arguments passed to the user-callback.

You can still use the old version of the callback, you are just required to tell paho-mqtt that you opt for this
version. For that just change your client creation from::

    # OLD code
    >>> mqttc = mqtt.Client()

to::

    # NEW code
    >>> mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)


That's it, the remaining of the code can stay unchanged.

Version 1 of the callback is deprecated, but is still supported in version 2.x. If you want to upgrade to the newer version of the API callback, you will need to update your callbacks:

on_connect
``````````

::

    # OLD code for MQTTv3
    def on_connect(client, userdata, flags, rc):
        if flags["session present"] == 1:
            # ...
        if rc == 0:
            # success connect
        if rc > 0:
            # error processing

    # OLD code for MQTTv5
    def on_connect(client, userdata, flags, reason_code, properties):
        if flags["session present"] == 1:
            # ...
        if reason_code == 0:
            # success connect

    # NEW code for both version
    def on_connect(client, userdata, flags, reason_code, properties):
        if flags.session_present:
            # ...
        if reason_code == 0:
            # success connect
        if reason_code > 0:
            # error processing


Be careful that for MQTTv3, ``rc`` (an integer) changed to ``reason_code`` (an instance of `ReasonCode`), and the numeric value changed.
The numeric value 0 means success for both, so as in above example, using ``reason_code == 0``, ``reason_code != 0`` or other comparison with zero
is fine.
But if you had comparison with other value, you will need to update the code. It's recommended to compare to string value::

    # OLD code for MQTTv3
    def on_connect(client, userdata, flags, rc):
        if rc == 1:
            # handle bad protocol version
        if rc == CONNACK_REFUSED_IDENTIFIER_REJECTED:
            # handle bad identifier

    # NEW code
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == "Unsupported protocol version":
            # handle bad protocol version
        if reason_code == "Client identifier not valid":
            # handle bad identifier

on_disconnect
`````````````

::

    # OLD code for MQTTv3
    def on_disconnect(client, userdata, rc):
        if rc == 0:
            # success disconnect
        if rc > 0:
            # error processing

    # OLD code for MQTTv5
    def on_disconnect(client, userdata, reason_code, properties):
        if reason_code:
            # error processing

    # NEW code for both version
    def on_disconnect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            # success disconnect
        if reason_code > 0:
            # error processing



on_subscribe
````````````

::

    # OLD code for MQTTv3
    def on_subscribe(client, userdata, mid, granted_qos):
        for sub_result in granted_qos:
            if sub_result == 1:
                # process QoS == 1
            if sub_result == 0x80:
                # error processing

    # OLD code for MQTTv5
    def on_subscribe(client, userdata, mid, granted_qos, rc, properties=None):
        for sub_result in reason_codes:
            if sub_result == 1:
                # process QoS == 1
            # Any reason code >= 128 is a failure.
            if sub_result >= 128:
                # error processing

    # NEW code for both version
    def on_subscribe(client, userdata, mid, reason_codes, properties):
        for sub_result in reason_codes:
            if sub_result == 1:
                # process QoS == 1
            # Any reason code >= 128 is a failure.
            if sub_result >= 128:
                # error processing



on_unsubscribe
``````````````

::

    # OLD code for MQTTv3
    def on_unsubscribe(client, userdata, mid):
        # ...

    # OLD code for MQTTv5
    def on_unsubscribe(client, userdata, mid, properties, reason_codes):
        # In OLD version, reason_codes could be a list or a single ReasonCode object
        if isinstance(reason_codes, list):
            for unsub_result in reason_codes:
                # Any reason code >= 128 is a failure.
                if reason_codes[0] >= 128:
                    # error processing
        else:
            # Any reason code >= 128 is a failure.
            if reason_codes > 128:
                # error processing


    # NEW code for both version
    def on_subscribe(client, userdata, mid, reason_codes, properties):
        # In NEW version, reason_codes is always a list. Empty for MQTTv3
        for unsub_result in reason_codes:
            # Any reason code >= 128 is a failure.
            if reason_codes[0] >= 128:
                # error processing


on_publish
``````````

::

    # OLD code
    def on_publish(client, userdata, mid):
        # ...


    # NEW code
    def on_publish(client, userdata, mid, reason_codes, properties):
        # ...



on_message
``````````

No change for this callback::

    # OLD & NEW code
    def on_message(client, userdata, message):
        # ...


Drop support for older Python
*****************************

paho-mqtt support Python 3.7 to 3.12. If you are using an older Python version, including
Python 2.x you will need to kept running the 1.x version of paho-mqtt.

Drop deprecated argument and method
***********************************

The following are dropped:

* ``max_packets`` argument in `loop()`, `loop_write()` and `loop_forever()` is removed
* ``force`` argument in `loop_stop()` is removed
* method ``message_retry_set()`` is removed

They were not used in previous version, so you can just remove them if you used them.

Stop exposing private function/class
************************************

Some private function or class are not longer exposed. The following are removed:

* function base62
* class WebsocketWrapper
* enum ConnectionState

Renamed ReasonCodes to ReasonCode
*********************************

The class ReasonCodes that was used to represent one reason code response from
broker or generated by the library is now named `ReasonCode`.

This should work without any change as ReasonCodes (plural, the old name) is still
present but deprecated.

Improved typing
***************

Version 2.0 improved typing, but this would be compatible with existing code.
The most likely issue are some integer that are now better type, like `dup` on MQTTMessage.

That means that code that used ``if msg.dup == 1:`` will need to be change to ``if msg.dup:`` (the later version
for with both paho-mqtt 1.x and 2.0).

Fix connect_srv
***************

`connect_srv()` didn't took the same argument as `connect()`. Fixed this, which means the signaure
changed. But since connect_srv was broken in previous version, this should not have any negative impact.

Added new properties
********************

The Client class added few new properties. If you are using a sub-class of Client and also defined a
attribute, method or properties with the same name, it will conflict.

The added properties are:

* `host`
* `port`
* `keepalive`
* `transport`
* `protocol`
* `connect_timeout`
* `username`
* `password`
* `max_inflight_messages`
* `max_queued_messages`
* `will_topic`
* `will_payload`
* `logger`
