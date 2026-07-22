"""
*******************************************************************
  Copyright (c) 2013, 2019 IBM Corp.

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v2.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v20.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

import logging
import queue
import sys
import threading
import time
import unittest
import unittest.mock
import warnings

import paho.mqtt
import paho.mqtt.client
from paho.mqtt.enums import CallbackAPIVersion, MQTTErrorCode
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties
from paho.mqtt.subscribeoptions import SubscribeOptions

DEFAULT_TIMEOUT = 5
# timeout for something that should not happen but we wait to
# give it time to happen if it does due to a bug.
WAIT_NON_EVENT_TIMEOUT = 1

class Callbacks:

    def __init__(self):
        self.messages = queue.Queue()
        self.publisheds = queue.Queue()
        self.subscribeds = queue.Queue()
        self.unsubscribeds  = queue.Queue()
        self.disconnecteds  = queue.Queue()
        self.connecteds  = queue.Queue()
        self.conn_failures  = queue.Queue()

    def __str__(self):
        return str(self.messages.queue) + str(self.messagedicts.queue) + str(self.publisheds.queue) + \
            str(self.subscribeds.queue) + \
            str(self.unsubscribeds.queue) + str(self.disconnects.queue)

    def clear(self):
        self.__init__()

    def on_connect(self, client, userdata, flags, reasonCode, properties):
        self.connecteds.put({"userdata": userdata, "flags": flags,
                             "reasonCode": reasonCode, "properties": properties})

    def on_connect_fail(self, client, userdata):
        self.conn_failures.put({"userdata": userdata})

    def wait_connect_fail(self):
        return self.conn_failures.get(timeout=10)

    def wait_connected(self):
        return self.connecteds.get(timeout=2)

    def on_disconnect(self, client, userdata, reasonCode, properties=None):
        self.disconnecteds.put(
            {"reasonCode": reasonCode, "properties": properties})

    def wait_disconnected(self):
        return self.disconnecteds.get(timeout=2)

    def on_message(self, client, userdata, message):
        self.messages.put({"userdata": userdata, "message": message})

    def published(self, client, userdata, msgid):
        self.publisheds.put(msgid)

    def wait_published(self):
        return self.publisheds.get(timeout=2)

    def on_subscribe(self, client, userdata, mid, reasonCodes, properties):
        self.subscribeds.put({"mid": mid, "userdata": userdata,
                              "properties": properties, "reasonCodes": reasonCodes})

    def wait_subscribed(self):
        return self.subscribeds.get(timeout=2)

    def unsubscribed(self, client, userdata, mid, properties, reasonCodes):
        self.unsubscribeds.put({"mid": mid, "userdata": userdata,
                                "properties": properties, "reasonCodes": reasonCodes})

    def wait_unsubscribed(self):
        return self.unsubscribeds.get(timeout=2)

    def on_log(self, client, userdata, level, buf):
        print(buf)

    def register(self, client):
        client.on_connect = self.on_connect
        client.on_subscribe = self.on_subscribe
        client.on_publish = self.published
        client.on_unsubscribe = self.unsubscribed
        client.on_message = self.on_message
        client.on_disconnect = self.on_disconnect
        client.on_connect_fail = self.on_connect_fail
        client.on_log = self.on_log

    def get_messages(self, count: int, timeout: float = DEFAULT_TIMEOUT):
        result = []
        deadline = time.time() + timeout
        while len(result) < count:
            get_timeout = deadline - time.time()
            if get_timeout <= 0:
                result.append(self.messages.get_nowait())
            else:
                result.append(self.messages.get(timeout=get_timeout))

        return result

    def get_at_most_messages(self, count: int, timeout: float = DEFAULT_TIMEOUT):
        result = []
        deadline = time.time() + timeout
        try:
            while len(result) < count:
                get_timeout = deadline - time.time()
                if get_timeout <= 0:
                    result.append(self.messages.get_nowait())
                else:
                    result.append(self.messages.get(timeout=get_timeout))
        except queue.Empty:
            pass

        return result


def cleanRetained(port):
    callback = Callbacks()
    curclient = paho.mqtt.client.Client(
        CallbackAPIVersion.VERSION1,
        b"clean retained",
        protocol=paho.mqtt.client.MQTTv5,
    )
    callback.register(curclient)
    curclient.connect(host="localhost", port=port)
    curclient.loop_start()
    callback.wait_connected()
    curclient.subscribe("#", options=SubscribeOptions(qos=0))
    callback.wait_subscribed()  # wait for retained messages to arrive
    try:
        while True:
            message = callback.messages.get(timeout=WAIT_NON_EVENT_TIMEOUT)
            if message["message"].payload != b"":
                logging.info("deleting retained message for topic", message["message"])
                curclient.publish(message["message"].topic, b"", 0, retain=True)
    except queue.Empty:
        pass
    curclient.disconnect()
    curclient.loop_stop()


def cleanup(port):
    # clean all client state
    print("clean up starting")
    clientids = ("aclient", "bclient")

    def _on_connect(client, *args):
        client.disconnect()

    for clientid in clientids:
        curclient = paho.mqtt.client.Client(
            CallbackAPIVersion.VERSION1,
            clientid.encode("utf-8"),
            protocol=paho.mqtt.client.MQTTv5,
        )
        curclient.on_connect = _on_connect
        curclient.connect(host="localhost", port=port, clean_start=True)
        curclient.loop_forever()

    # clean retained messages
    cleanRetained(port)
    print("clean up finished")


class _TestBase(unittest.TestCase):

    ClientClass = paho.mqtt.client.Client
    CallbacksClass = Callbacks
    @classmethod
    def new_client(cls, clientid):
        callback = cls.CallbacksClass()
        client = cls.ClientClass(
            CallbackAPIVersion.VERSION1,
            clientid.encode("utf-8"),
            protocol=paho.mqtt.client.MQTTv5,
        )
        callback.register(client)
        client.loop_start()
        return client, callback

    @classmethod
    def setUpClass(cls):
        global callback, callback2, aclient, bclient

        sys.path.append("paho.mqtt.testing/interoperability/")
        try:
            import mqtt.brokers  # noqa: F401  (populates sys.modules)
        except ImportError as ie:
            raise unittest.SkipTest("paho.mqtt.testing not present.") from ie

        cls._start_broker(port=0)
        setData()
        cleanup(cls._test_broker_port)

        callback = Callbacks()
        callback2 = Callbacks()

        aclient = paho.mqtt.client.Client(CallbackAPIVersion.VERSION1, b"aclient", protocol=paho.mqtt.client.MQTTv5)
        callback.register(aclient)

        bclient = paho.mqtt.client.Client(CallbackAPIVersion.VERSION1, b"bclient", protocol=paho.mqtt.client.MQTTv5)
        callback2.register(bclient)

    @classmethod
    def _start_broker(cls, port):
        """(Re)start mqtt.brokers.run in a daemon thread bound to `port`
        (0 = OS picks one). Blocks until the listener is bound and sets
        cls._test_broker / cls._test_broker_port."""
        import socketserver

        import mqtt.brokers
        # Make sure the listening socket can be rebound immediately after
        # server_close(), regardless of paho.mqtt.testing's own defaults.
        socketserver.TCPServer.allow_reuse_address = True

        with unittest.mock.patch("signal.signal", unittest.mock.MagicMock()):
            cls._test_broker = threading.Thread(
                target=mqtt.brokers.run,
                kwargs={"config": [f"listener {port}"]},
            )
            cls._test_broker.daemon = True
            cls._test_broker.start()

            for _ in range(50):
                time.sleep(0.1)
                server = mqtt.brokers.listeners.TCPListeners.server
                if server is not None:
                    bound_port = server.socket.getsockname()[1]
                    if bound_port != 0:
                        cls._test_broker_port = bound_port
                        return
            raise ValueError("can't find the test broker port")

    @classmethod
    def tearDownClass(cls):
        # Another hack to stop the test broker... we rely on fact that it use a sockserver.TCPServer
        import mqtt.brokers
        mqtt.brokers.listeners.TCPListeners.server.shutdown()
        cls._test_broker.join(5)

    @classmethod
    def _reboot_broker(cls):
        import mqtt.brokers
        server = mqtt.brokers.listeners.TCPListeners.server
        server.shutdown()
        server.server_close()
        cls._test_broker.join(5)
        cls._start_broker(port=cls._test_broker_port)


class Test(_TestBase):

    def test_basic(self):
        import datetime
        print(datetime.datetime.now(), "start")
        aclient.connect(host="localhost", port=self._test_broker_port)
        aclient.loop_start()
        print(datetime.datetime.now(), "loop_start")
        response = callback.wait_connected()
        print(datetime.datetime.now(), "connected")
        self.assertEqual(response["reasonCode"].getName(), "Success")

        aclient.subscribe(topics[0], options=SubscribeOptions(qos=2))
        response = callback.wait_subscribed()
        print(datetime.datetime.now(), "wait_subscribed")
        self.assertEqual(response["reasonCodes"][0].getName(), "Granted QoS 2")

        aclient.publish(topics[0], b"qos 0")
        aclient.publish(topics[0], b"qos 1", 1)
        aclient.publish(topics[0], b"qos 2", 2)

        msgs = callback.get_messages(3)
        print(datetime.datetime.now(), "publish get")
        got_payload = {
            x["message"].payload
            for x in msgs
        }

        self.assertEqual(got_payload, {b"qos 0", b"qos 1", b"qos 2"})
        aclient.disconnect()

        callback.clear()
        aclient.loop_stop()

    def test_connect_fail(self):
        clientid = "connection failure"

        fclient, fcallback = self.new_client(clientid)

        fclient.user_data_set(1)
        fclient.connect_async("localhost", 1)
        response = fcallback.wait_connect_fail()
        self.assertEqual(response["userdata"], 1)
        fclient.loop_stop()

    def test_retained_message(self):

        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.UserProperty = ("a", "2")
        publish_properties.UserProperty = ("c", "3")

        # retained messages
        callback.clear()
        aclient.connect(host="localhost", port=self._test_broker_port)
        aclient.loop_start()
        response = callback.wait_connected()
        aclient.publish(topics[1], b"qos 0", 0,
                        retain=True, properties=publish_properties)
        aclient.publish(topics[2], b"qos 1", 1,
                        retain=True, properties=publish_properties)
        aclient.publish(topics[3], b"qos 2", 2,
                        retain=True, properties=publish_properties)
        # wait until those messages are published
        time.sleep(WAIT_NON_EVENT_TIMEOUT)
        aclient.subscribe(wildtopics[5], options=SubscribeOptions(qos=2))
        response = callback.wait_subscribed()
        self.assertEqual(response["reasonCodes"][0].getName(), "Granted QoS 2")
        msgs = callback.get_messages(3)

        aclient.disconnect()
        aclient.loop_stop()

        self.assertTrue(callback.messages.empty())

        userprops = msgs[0]["message"].properties.UserProperty
        self.assertTrue(userprops in [[("a", "2"), ("c", "3")], [
                        ("c", "3"), ("a", "2")]], userprops)
        userprops = msgs[1]["message"].properties.UserProperty
        self.assertTrue(userprops in [[("a", "2"), ("c", "3")], [
                        ("c", "3"), ("a", "2")]], userprops)
        userprops = msgs[2]["message"].properties.UserProperty
        self.assertTrue(userprops in [[("a", "2"), ("c", "3")], [
                        ("c", "3"), ("a", "2")]], userprops)
        qoss = [x["message"].qos for x in msgs]
        self.assertTrue(1 in qoss and 2 in qoss and 0 in qoss, qoss)

        cleanRetained(self._test_broker_port)

    def test_will_message(self):
        # will messages and keep alive
        callback.clear()
        callback2.clear()
        self.assertTrue(callback2.messages.empty(), callback2.messages.queue)

        will_properties = Properties(PacketTypes.WILLMESSAGE)
        will_properties.WillDelayInterval = 0  # this is the default anyway
        will_properties.UserProperty = ("a", "2")
        will_properties.UserProperty = ("c", "3")

        aclient.will_set(topics[2], payload=b"will message",
                         properties=will_properties)

        aclient.connect(host="localhost", port=self._test_broker_port, keepalive=2)
        aclient.loop_start()
        response = callback.wait_connected()
        bclient.connect(host="localhost", port=self._test_broker_port)
        bclient.loop_start()
        response = callback2.wait_connected()
        bclient.subscribe(topics[2], qos=2)
        response = callback2.wait_subscribed()
        self.assertEqual(response["reasonCodes"][0].getName(), "Granted QoS 2")

        # keep alive timeout ought to be triggered so the will message is received
        aclient.loop_stop()  # so that pings aren't sent
        msg = callback2.messages.get(timeout=10)
        bclient.disconnect()
        bclient.loop_stop()

        props = msg["message"].properties
        self.assertEqual(props.UserProperty, [("a", "2"), ("c", "3")])

    def test_zero_length_clientid(self):
        logging.info("Zero length clientid test starting")

        callback0 = Callbacks()

        client0 = paho.mqtt.client.Client(CallbackAPIVersion.VERSION1, protocol=paho.mqtt.client.MQTTv5)
        callback0.register(client0)
        client0.loop_start()
        # should not be rejected
        client0.connect(host="localhost", port=self._test_broker_port, clean_start=False)
        response = callback0.wait_connected()
        self.assertEqual(response["reasonCode"].getName(), "Success")
        self.assertTrue(
            len(response["properties"].AssignedClientIdentifier) > 0)
        client0.disconnect()
        client0.loop_stop()

        client0 = paho.mqtt.client.Client(CallbackAPIVersion.VERSION1, protocol=paho.mqtt.client.MQTTv5)
        callback0.register(client0)
        client0.loop_start()
        client0.connect(host="localhost", port=self._test_broker_port)  # should work
        response = callback0.wait_connected()
        self.assertEqual(response["reasonCode"].getName(), "Success")
        self.assertTrue(
            len(response["properties"].AssignedClientIdentifier) > 0)
        client0.disconnect()
        client0.loop_stop()

        # when we supply a client id, we should not get one assigned
        client0 = paho.mqtt.client.Client(
            CallbackAPIVersion.VERSION1, "client0", protocol=paho.mqtt.client.MQTTv5,
        )
        callback0.register(client0)
        client0.loop_start()
        client0.connect(host="localhost", port=self._test_broker_port)  # should work
        response = callback0.wait_connected()
        self.assertEqual(response["reasonCode"].getName(), "Success")
        self.assertFalse(
            hasattr(response["properties"], "AssignedClientIdentifier"))
        client0.disconnect()
        client0.loop_stop()

    def test_offline_message_queueing(self):
        # message queueing for offline clients
        cleanRetained(self._test_broker_port)
        ocallback = Callbacks()
        clientid = b"offline message queueing"

        oclient = paho.mqtt.client.Client(
            CallbackAPIVersion.VERSION1, clientid, protocol=paho.mqtt.client.MQTTv5,
        )
        ocallback.register(oclient)
        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.SessionExpiryInterval = 99999
        oclient.loop_start()
        oclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        ocallback.wait_connected()
        oclient.subscribe(wildtopics[5], qos=2)
        ocallback.wait_subscribed()
        oclient.disconnect()
        oclient.loop_stop()

        bclient.loop_start()
        bclient.connect(host="localhost", port=self._test_broker_port)
        callback2.wait_connected()
        msg1 = bclient.publish(topics[1], b"qos 0", 0)
        msg2 = bclient.publish(topics[2], b"qos 1", 1)
        msg3 = bclient.publish(topics[3], b"qos 2", 2)

        msg1.wait_for_publish()
        msg2.wait_for_publish()
        msg3.wait_for_publish()

        bclient.disconnect()
        bclient.loop_stop()

        oclient = paho.mqtt.client.Client(
            CallbackAPIVersion.VERSION1, clientid, protocol=paho.mqtt.client.MQTTv5,
        )
        ocallback.register(oclient)
        oclient.loop_start()
        oclient.connect(host="localhost", port=self._test_broker_port, clean_start=False)
        ocallback.wait_connected()

        msgs = ocallback.get_at_most_messages(3)

        oclient.disconnect()
        oclient.loop_stop()

        self.assertTrue(len(msgs) in [
                        2, 3], ocallback.messages.qsize())
        logging.info("This server %s queueing QoS 0 messages for offline clients" %
                     ("is" if len(msgs) == 3 else "is not"))

    def test_overlapping_subscriptions(self):
        # overlapping subscriptions. When there is more than one matching subscription for the same client for a topic,
        # the server may send back one message with the highest QoS of any matching subscription, or one message for
        # each subscription with a matching QoS.
        ocallback = Callbacks()
        clientid = b"overlapping subscriptions"

        oclient = paho.mqtt.client.Client(
            CallbackAPIVersion.VERSION1, clientid, protocol=paho.mqtt.client.MQTTv5,
        )
        ocallback.register(oclient)

        oclient.loop_start()
        oclient.connect(host="localhost", port=self._test_broker_port)
        ocallback.wait_connected()
        oclient.subscribe([(wildtopics[6], SubscribeOptions(qos=2)),
                           (wildtopics[0], SubscribeOptions(qos=1))])
        ocallback.wait_subscribed()
        oclient.publish(topics[3], b"overlapping topic filters", 2)
        ocallback.wait_published()

        msgs = ocallback.get_at_most_messages(2)
        if len(msgs) == 1:
            logging.info(
                "This server is publishing one message for all matching overlapping subscriptions, not one for each.")
            self.assertEqual(
                msgs[0]["message"].qos, 2, msgs[0]["message"].qos)
        else:
            logging.info(
                "This server is publishing one message per each matching overlapping subscription.")
            self.assertTrue((msgs[0]["message"].qos == 2 and msgs[1]["message"].qos == 1) or
                            (msgs[0]["message"].qos == 1 and msgs[1]["message"].qos == 2), msgs)
        oclient.disconnect()
        oclient.loop_stop()
        ocallback.clear()

    def test_subscribe_failure(self):
        # Subscribe failure.  A new feature of MQTT 3.1.1 is the ability to send back negative responses to subscribe
        # requests.  One way of doing this is to subscribe to a topic which is not allowed to be subscribed to.
        logging.info("Subscribe failure test starting")

        ocallback = Callbacks()
        clientid = b"subscribe failure"
        oclient = paho.mqtt.client.Client(
            CallbackAPIVersion.VERSION1, clientid, protocol=paho.mqtt.client.MQTTv5,
        )
        ocallback.register(oclient)
        oclient.loop_start()
        oclient.connect(host="localhost", port=self._test_broker_port)
        ocallback.wait_connected()
        oclient.subscribe(nosubscribe_topics[0], qos=2)
        response = ocallback.wait_subscribed()

        self.assertEqual(response["reasonCodes"][0].getName(), "Unspecified error",
                         f"return code should be 0x80 {response['reasonCodes'][0].getName()}")
        oclient.disconnect()
        oclient.loop_stop()

    def test_unsubscribe(self):
        callback2.clear()
        bclient.connect(host="localhost", port=self._test_broker_port)
        bclient.loop_start()
        callback2.wait_connected()
        bclient.subscribe(topics[0], qos=2)
        callback2.wait_subscribed()
        bclient.subscribe(topics[1], qos=2)
        callback2.wait_subscribed()
        bclient.subscribe(topics[2], qos=2)
        callback2.wait_subscribed()
        time.sleep(1)  # wait for any retained messages, hopefully
        # Unsubscribe from one topic
        bclient.unsubscribe(topics[0])
        callback2.wait_unsubscribed()
        callback2.clear()  # if there were any retained messages

        aclient.connect(host="localhost", port=self._test_broker_port)
        aclient.loop_start()
        callback.wait_connected()
        aclient.publish(topics[0], b"topic 0 - unsubscribed", 1, retain=False)
        aclient.publish(topics[1], b"topic 1", 1, retain=False)
        aclient.publish(topics[2], b"topic 2", 1, retain=False)

        msgs = callback2.get_messages(2)

        bclient.disconnect()
        bclient.loop_stop()
        aclient.disconnect()
        aclient.loop_stop()
        self.assertEqual(len(msgs), 2)


    def test_session_expiry(self):
        # no session expiry property == never expire

        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.SessionExpiryInterval = 0  # expire immediately

        clientid = "session expiry"

        eclient, ecallback = self.new_client(clientid)

        eclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        connack = ecallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)
        eclient.subscribe(topics[0], qos=2)
        ecallback.wait_subscribed()
        eclient.disconnect()
        ecallback.wait_disconnected()
        eclient.loop_stop()

        fclient, fcallback = self.new_client(clientid)

        # session should immediately expire
        fclient.connect_async(host="localhost", port=self._test_broker_port, clean_start=False,
                              properties=connect_properties)
        connack = fcallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)
        fclient.disconnect()
        fcallback.wait_disconnected()

        connect_properties.SessionExpiryInterval = 5

        eclient, ecallback = self.new_client(clientid)

        eclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        connack = ecallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)
        eclient.subscribe(topics[0], qos=2)
        ecallback.wait_subscribed()
        eclient.disconnect()
        ecallback.wait_disconnected()
        eclient.loop_stop()

        time.sleep(2)
        # session should still exist
        fclient, fcallback = self.new_client(clientid)
        fclient.connect(host="localhost", port=self._test_broker_port, clean_start=False,
                        properties=connect_properties)
        connack = fcallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], True)
        fclient.disconnect()
        fcallback.wait_disconnected()
        fclient.loop_stop()

        time.sleep(6)
        # session should not exist
        fclient, fcallback = self.new_client(clientid)
        fclient.connect(host="localhost", port=self._test_broker_port, clean_start=False,
                        properties=connect_properties)
        connack = fcallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)
        fclient.disconnect()
        fcallback.wait_disconnected()
        fclient.loop_stop()

        eclient, ecallback = self.new_client(clientid)
        connect_properties.SessionExpiryInterval = 1
        connack = eclient.connect(
            host="localhost", port=self._test_broker_port, properties=connect_properties)
        connack = ecallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)
        eclient.subscribe(topics[0], qos=2)
        ecallback.wait_subscribed()
        disconnect_properties = Properties(PacketTypes.DISCONNECT)
        disconnect_properties.SessionExpiryInterval = 5
        eclient.disconnect(properties=disconnect_properties)
        ecallback.wait_disconnected()
        eclient.loop_stop()

        time.sleep(3)
        # session should still exist as we changed the expiry interval on disconnect
        fclient, fcallback = self.new_client(clientid)
        fclient.connect(host="localhost", port=self._test_broker_port, clean_start=False,
                        properties=connect_properties)
        connack = fcallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], True)
        disconnect_properties.SessionExpiryInterval = 0
        fclient.disconnect(properties=disconnect_properties)
        fcallback.wait_disconnected()
        fclient.loop_stop()

        # session should immediately expire
        fclient, fcallback = self.new_client(clientid)
        fclient.connect(host="localhost", port=self._test_broker_port, clean_start=False,
                        properties=connect_properties)
        connack = fcallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)
        fclient.disconnect()
        fcallback.wait_disconnected()
        fclient.loop_stop()

        fclient.loop_stop()
        eclient.loop_stop()

    def test_user_properties(self):
        clientid = "user properties"
        uclient, ucallback = self.new_client(clientid)
        uclient.loop_start()
        uclient.connect(host="localhost", port=self._test_broker_port)
        ucallback.wait_connected()

        uclient.subscribe(topics[0], qos=2)
        ucallback.wait_subscribed()

        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.UserProperty = ("a", "2")
        publish_properties.UserProperty = ("c", "3")
        uclient.publish(topics[0], b"", 0, retain=False,
                        properties=publish_properties)
        uclient.publish(topics[0], b"", 1, retain=False,
                        properties=publish_properties)
        uclient.publish(topics[0], b"", 2, retain=False,
                        properties=publish_properties)

        msgs = ucallback.get_messages(3)

        uclient.disconnect()
        ucallback.wait_disconnected()
        uclient.loop_stop()
        self.assertTrue(ucallback.messages.empty(), ucallback.messages.queue)
        userprops = msgs[0]["message"].properties.UserProperty
        self.assertTrue(userprops in [[("a", "2"), ("c", "3")], [
                        ("c", "3"), ("a", "2")]], userprops)
        userprops = msgs[1]["message"].properties.UserProperty
        self.assertTrue(userprops in [[("a", "2"), ("c", "3")], [
                        ("c", "3"), ("a", "2")]], userprops)
        userprops = msgs[2]["message"].properties.UserProperty
        self.assertTrue(userprops in [[("a", "2"), ("c", "3")], [
                        ("c", "3"), ("a", "2")]], userprops)
        qoss = [x["message"].qos for x in msgs]
        self.assertTrue(1 in qoss and 2 in qoss and 0 in qoss, qoss)

    def test_payload_format(self):
        clientid = "payload format"
        pclient, pcallback = self.new_client(clientid)
        pclient.loop_start()
        pclient.connect_async(host="localhost", port=self._test_broker_port)
        pcallback.wait_connected()

        pclient.subscribe(topics[0], qos=2)
        pcallback.wait_subscribed()
        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.PayloadFormatIndicator = 1
        publish_properties.ContentType = "My name"
        info = pclient.publish(
            topics[0], b"qos 0", 0, retain=False, properties=publish_properties)
        info.wait_for_publish()
        info = pclient.publish(
            topics[0], b"qos 1", 1, retain=False, properties=publish_properties)
        info.wait_for_publish()
        info = pclient.publish(
            topics[0], b"qos 2", 2, retain=False, properties=publish_properties)
        info.wait_for_publish()

        msgs = pcallback.get_messages(3)

        pclient.disconnect()
        pcallback.wait_disconnected()
        pclient.loop_stop()

        self.assertTrue(pcallback.messages.empty(), pcallback.messages.queue)
        props = msgs[0]["message"].properties
        self.assertEqual(props.ContentType, "My name", props.ContentType)
        self.assertEqual(props.PayloadFormatIndicator,
                         1, props.PayloadFormatIndicator)
        props = msgs[1]["message"].properties
        self.assertEqual(props.ContentType, "My name", props.ContentType)
        self.assertEqual(props.PayloadFormatIndicator,
                         1, props.PayloadFormatIndicator)
        props = msgs[2]["message"].properties
        self.assertEqual(props.ContentType, "My name", props.ContentType)
        self.assertEqual(props.PayloadFormatIndicator,
                         1, props.PayloadFormatIndicator)
        qoss = [x["message"].qos for x in msgs]
        self.assertTrue(1 in qoss and 2 in qoss and 0 in qoss, qoss)

    def test_message_expiry(self):
        clientid = "message expiry"

        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.SessionExpiryInterval = 99999

        lbclient, lbcallback = self.new_client(f"{clientid} b")
        lbclient.loop_start()
        lbclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        lbcallback.wait_connected()
        lbclient.subscribe(topics[0], qos=2)
        lbcallback.wait_subscribed()
        disconnect_properties = Properties(PacketTypes.DISCONNECT)
        disconnect_properties.SessionExpiryInterval = 999999999
        lbclient.disconnect(properties=disconnect_properties)
        lbcallback.wait_disconnected()
        lbclient.loop_stop()

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.loop_start()
        laclient.connect(host="localhost", port=self._test_broker_port)
        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.MessageExpiryInterval = 1
        laclient.publish(topics[0], b"qos 1 - expire", 1,
                         retain=False, properties=publish_properties)
        laclient.publish(topics[0], b"qos 2 - expire", 2,
                         retain=False, properties=publish_properties)

        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.MessageExpiryInterval = 6
        laclient.publish(topics[0], b"qos 1 - don't expire",
                         1, retain=False, properties=publish_properties)
        laclient.publish(topics[0], b"qos 2 - don't expire",
                         2, retain=False, properties=publish_properties)

        time.sleep(3)
        lbclient, lbcallback = self.new_client(f"{clientid} b")
        lbclient.loop_start()
        lbclient.connect(host="localhost", port=self._test_broker_port, clean_start=False)
        lbcallback.wait_connected()

        msgs = lbcallback.get_messages(2)

        self.assertTrue(lbcallback.messages.empty(), lbcallback.messages.queue)
        self.assertTrue(msgs[0]["message"].properties.MessageExpiryInterval < 6,
                        msgs[0]["message"].properties.MessageExpiryInterval)
        self.assertTrue(msgs[1]["message"].properties.MessageExpiryInterval < 6,
                        msgs[1]["message"].properties.MessageExpiryInterval)
        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

        lbclient.disconnect()
        lbcallback.wait_disconnected()
        lbclient.loop_stop()

    def test_subscribe_options(self):
        # noLocal
        clientid = 'subscribe options - noLocal'

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        lacallback.wait_connected()
        laclient.loop_start()
        laclient.subscribe(
            topics[0], options=SubscribeOptions(qos=2, noLocal=True))
        lacallback.wait_subscribed()

        lbclient, lbcallback = self.new_client(f"{clientid} b")
        lbclient.connect(host="localhost", port=self._test_broker_port)
        lbcallback.wait_connected()
        lbclient.loop_start()
        lbclient.subscribe(
            topics[0], options=SubscribeOptions(qos=2, noLocal=True))
        lbcallback.wait_subscribed()

        laclient.publish(topics[0], b"noLocal test", 1, retain=False)

        lbcallback.messages.get(timeout=DEFAULT_TIMEOUT)
        try:
            lacallback.messages.get(timeout=WAIT_NON_EVENT_TIMEOUT)
            raise ValueError("unexpected message received")
        except queue.Empty:
            pass

        self.assertTrue(lacallback.messages.empty(), lacallback.messages.queue)
        self.assertTrue(lbcallback.messages.empty(), lbcallback.messages.queue)
        laclient.disconnect()
        lacallback.wait_disconnected()
        lbclient.disconnect()
        lbcallback.wait_disconnected()
        laclient.loop_stop()
        lbclient.loop_stop()

        # retainAsPublished
        clientid = 'subscribe options - retain as published'
        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        lacallback.wait_connected()
        laclient.subscribe(topics[0], options=SubscribeOptions(
            qos=2, retainAsPublished=True))
        lacallback.wait_subscribed()
        laclient.publish(
            topics[0], b"retain as published false", 1, retain=False)
        laclient.publish(
            topics[0], b"retain as published true", 1, retain=True)

        msgs = lacallback.get_messages(2)

        self.assertTrue(lacallback.messages.empty(), lacallback.messages.queue)
        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()
        self.assertEqual(msgs[0]["message"].retain, False)
        self.assertEqual(msgs[1]["message"].retain, True)

        # retainHandling
        clientid = 'subscribe options - retain handling'
        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        lacallback.wait_connected()
        laclient.publish(topics[1], b"qos 0", 0, retain=True)
        laclient.publish(topics[2], b"qos 1", 1, retain=True)
        laclient.publish(topics[3], b"qos 2", 2, retain=True)
        time.sleep(1)

        # retain handling 1 only gives us retained messages on a new subscription
        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=1))
        lacallback.wait_subscribed()

        msgs = lacallback.get_messages(3)

        self.assertTrue(lacallback.messages.empty())
        qoss = [x["message"].qos for x in msgs]
        self.assertTrue(1 in qoss and 2 in qoss and 0 in qoss, qoss)
        lacallback.clear()
        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=1))
        lacallback.wait_subscribed()
        time.sleep(1)
        self.assertTrue(lacallback.messages.empty())

        # remove that subscription
        properties = Properties(PacketTypes.UNSUBSCRIBE)
        properties.UserProperty = ("a", "2")
        properties.UserProperty = ("c", "3")
        laclient.unsubscribe(wildtopics[5], properties)
        lacallback.wait_unsubscribed()

        # check that we really did remove that subscription
        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=1))
        lacallback.wait_subscribed()
        msgs = lacallback.get_messages(3)
        qoss = [x["message"].qos for x in msgs]
        self.assertTrue(1 in qoss and 2 in qoss and 0 in qoss, qoss)
        lacallback.clear()
        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=1))
        lacallback.wait_subscribed()
        time.sleep(WAIT_NON_EVENT_TIMEOUT)
        self.assertTrue(lacallback.messages.empty())

        # remove that subscription
        properties = Properties(PacketTypes.UNSUBSCRIBE)
        properties.UserProperty = ("a", "2")
        properties.UserProperty = ("c", "3")
        laclient.unsubscribe(wildtopics[5], properties)
        lacallback.wait_unsubscribed()

        lacallback.clear()
        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=2))
        lacallback.wait_subscribed()
        self.assertTrue(lacallback.messages.empty())
        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=2))
        lacallback.wait_subscribed()
        self.assertTrue(lacallback.messages.empty())

        # remove that subscription
        laclient.unsubscribe(wildtopics[5])
        lacallback.wait_unsubscribed()

        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=0))
        lacallback.wait_subscribed()
        msgs = lacallback.get_messages(3)
        qoss = [x["message"].qos for x in msgs]
        self.assertTrue(1 in qoss and 2 in qoss and 0 in qoss, qoss)
        lacallback.clear()
        laclient.subscribe(
            wildtopics[5], options=SubscribeOptions(2, retainHandling=0))
        msgs = lacallback.get_messages(3)
        qoss = [x["message"].qos for x in msgs]
        self.assertTrue(1 in qoss and 2 in qoss and 0 in qoss, qoss)
        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

        cleanRetained(self._test_broker_port)

    def test_subscription_identifiers(self):
        clientid = 'subscription identifiers'

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        lacallback.wait_connected()
        laclient.loop_start()

        sub_properties = Properties(PacketTypes.SUBSCRIBE)
        sub_properties.SubscriptionIdentifier = 456789
        laclient.subscribe(topics[0], qos=2, properties=sub_properties)
        lacallback.wait_subscribed()

        lbclient, lbcallback = self.new_client(f"{clientid} b")
        lbclient.connect(host="localhost", port=self._test_broker_port)
        lbcallback.wait_connected()
        lbclient.loop_start()
        sub_properties = Properties(PacketTypes.SUBSCRIBE)
        sub_properties.SubscriptionIdentifier = 2
        lbclient.subscribe(topics[0], qos=2, properties=sub_properties)
        lbcallback.wait_subscribed()

        sub_properties.clear()
        sub_properties.SubscriptionIdentifier = 3
        lbclient.subscribe(f"{topics[0]}/#", qos=2, properties=sub_properties)

        lbclient.publish(topics[0], b"sub identifier test", 1, retain=False)

        msg = lacallback.messages.get(timeout=DEFAULT_TIMEOUT)
        self.assertTrue(lacallback.messages.empty(), lacallback.messages.queue)
        self.assertEqual(msg["message"].properties.SubscriptionIdentifier[0],
                         456789, msg["message"].properties.SubscriptionIdentifier)
        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

        msg = lbcallback.messages.get(timeout=DEFAULT_TIMEOUT)
        self.assertTrue(lbcallback.messages.empty(), lbcallback.messages.queue)
        expected_subsids = {2, 3}
        received_subsids = set(
            msg["message"].properties.SubscriptionIdentifier)
        self.assertEqual(received_subsids, expected_subsids, received_subsids)
        lbclient.disconnect()
        lbcallback.wait_disconnected()
        lbclient.loop_stop()

    def test_request_response(self):
        clientid = 'request response'

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        lacallback.wait_connected()
        laclient.loop_start()

        lbclient, lbcallback = self.new_client(f"{clientid} b")
        lbclient.connect(host="localhost", port=self._test_broker_port)
        lbcallback.wait_connected()
        lbclient.loop_start()

        laclient.subscribe(
            topics[0], options=SubscribeOptions(2, noLocal=True))
        lacallback.wait_subscribed()

        lbclient.subscribe(
            topics[0], options=SubscribeOptions(2, noLocal=True))
        lbcallback.wait_subscribed()

        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.ResponseTopic = topics[0]
        publish_properties.CorrelationData = b"334"
        # client a is the requester
        laclient.publish(topics[0], b"request", 1,
                         properties=publish_properties)

        # client b is the responder
        msg = lbcallback.messages.get(timeout=DEFAULT_TIMEOUT)
        self.assertEqual(msg["message"].properties.ResponseTopic, topics[0],
                         msg["message"].properties)
        self.assertEqual(msg["message"].properties.CorrelationData, b"334",
                         msg["message"].properties)

        lbclient.publish(msg["message"].properties.ResponseTopic, b"response", 1,
                         properties=msg["message"].properties)

        # client a gets the response
        lacallback.messages.get(timeout=DEFAULT_TIMEOUT)

        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()
        lbclient.disconnect()
        lbcallback.wait_disconnected()
        lbclient.loop_stop()

    def test_client_topic_alias(self):
        clientid = 'client topic alias'

        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.TopicAliasMaximum = 0  # server topic aliases not allowed
        connect_properties.SessionExpiryInterval = 99999
        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        connack = lacallback.wait_connected()
        clientTopicAliasMaximum = 0
        if hasattr(connack["properties"], "TopicAliasMaximum"):
            clientTopicAliasMaximum = connack["properties"].TopicAliasMaximum

        if clientTopicAliasMaximum == 0:
            laclient.disconnect()
            lacallback.wait_disconnected()
            laclient.loop_stop()
            warnings.warn(
                f"Exiting test_client_topic_alias early. Broker does not support Topic alii. {clientTopicAliasMaximum=}",
                stacklevel=2,
            )
            return

        laclient.subscribe(topics[0], qos=2)
        lacallback.wait_subscribed()

        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.TopicAlias = 1
        laclient.publish(topics[0], b"topic alias 1",
                         1, properties=publish_properties)
        lacallback.messages.get(timeout=DEFAULT_TIMEOUT)

        laclient.publish("", b"topic alias 2", 1,
                         properties=publish_properties)
        lacallback.messages.get(timeout=DEFAULT_TIMEOUT)

        laclient.disconnect()  # should get rid of the topic aliases but not subscriptions
        lacallback.wait_disconnected()
        laclient.loop_stop()

        # check aliases have been deleted
        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port, clean_start=False,
                         properties=connect_properties)

        laclient.publish(topics[0], b"topic alias 3", 1)
        lacallback.messages.get(timeout=DEFAULT_TIMEOUT)

        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.TopicAlias = 1
        laclient.publish("", b"topic alias 4", 1,
                         properties=publish_properties)

        # should get back a disconnect with Topic alias invalid
        lacallback.wait_disconnected()
        laclient.loop_stop()

    def test_server_topic_alias(self):
        clientid = 'server topic alias'

        serverTopicAliasMaximum = 1  # server topic alias allowed
        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.TopicAliasMaximum = serverTopicAliasMaximum

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        lacallback.wait_connected()
        laclient.loop_start()

        laclient.subscribe(topics[0], qos=2)
        lacallback.wait_subscribed()

        for qos in range(3):
            laclient.publish(topics[0], b"topic alias 1", qos)
        msgs = lacallback.get_messages(3)
        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

        # first message should set the topic alias
        self.assertTrue(hasattr(
            msgs[0]["message"].properties, "TopicAlias"), msgs[0]["message"].properties)
        topicalias = msgs[0]["message"].properties.TopicAlias

        self.assertTrue(topicalias > 0)
        self.assertEqual(msgs[0]["message"].topic, topics[0])

        self.assertEqual(
            msgs[1]["message"].properties.TopicAlias, topicalias)
        self.assertEqual(msgs[1]["message"].topic, "")

        self.assertEqual(
            msgs[2]["message"].properties.TopicAlias, topicalias)
        self.assertEqual(msgs[2]["message"].topic, "")

        serverTopicAliasMaximum = 0  # no server topic alias allowed
        connect_properties = Properties(PacketTypes.CONNECT)
        # connect_properties.TopicAliasMaximum = serverTopicAliasMaximum # default is 0

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        lacallback.wait_connected()
        laclient.loop_start()

        laclient.subscribe(topics[0], qos=2)
        lacallback.wait_subscribed()

        for qos in range(3):
            laclient.publish(topics[0], b"topic alias 2", qos)
        msgs = lacallback.get_messages(3)
        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

        # No topic aliases
        self.assertFalse(hasattr(
            msgs[0]["message"].properties, "TopicAlias"), msgs[0]["message"].properties)
        self.assertFalse(hasattr(
            msgs[1]["message"].properties, "TopicAlias"), msgs[1]["message"].properties)
        self.assertFalse(hasattr(
            msgs[2]["message"].properties, "TopicAlias"), msgs[2]["message"].properties)

        serverTopicAliasMaximum = 0  # no server topic alias allowed
        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.TopicAliasMaximum = serverTopicAliasMaximum  # default is 0

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        lacallback.wait_connected()
        laclient.loop_start()

        laclient.subscribe(topics[0], qos=2)
        lacallback.wait_subscribed()

        for qos in range(3):
            laclient.publish(topics[0], b"topic alias 3", qos)
        msgs = lacallback.get_messages(3)
        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

        # No topic aliases
        self.assertFalse(hasattr(
            msgs[0]["message"].properties, "TopicAlias"), msgs[0]["message"].properties)
        self.assertFalse(hasattr(
            msgs[1]["message"].properties, "TopicAlias"), msgs[1]["message"].properties)
        self.assertFalse(hasattr(
            msgs[2]["message"].properties, "TopicAlias"), msgs[2]["message"].properties)

    def test_maximum_packet_size(self):
        clientid = 'maximum packet size'

        # 1. server max packet size
        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        connack = lacallback.wait_connected()
        laclient.loop_start()

        serverMaximumPacketSize = 2**28-1
        if hasattr(connack["properties"], "MaximumPacketSize"):
            serverMaximumPacketSize = connack["properties"].MaximumPacketSize

        if serverMaximumPacketSize < 65535:
            # publish bigger packet than server can accept
            payload = b"."*serverMaximumPacketSize
            laclient.publish(topics[0], payload, 0)
            # should get back a disconnect with packet size too big
            response = lacallback.wait_disconnected()
            self.assertEqual(response["reasonCode"].getName(),
                             "Packet too large", response["reasonCode"].getName())
        else:
            laclient.disconnect()
            lacallback.wait_disconnected()
        laclient.loop_stop()

        # 1. client max packet size
        maximumPacketSize = 64  # max packet size we want to receive
        connect_properties = Properties(PacketTypes.CONNECT)
        connect_properties.MaximumPacketSize = maximumPacketSize

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        connack = lacallback.wait_connected()
        laclient.loop_start()

        serverMaximumPacketSize = 2**28-1
        if hasattr(connack["properties"], "MaximumPacketSize"):
            serverMaximumPacketSize = connack["properties"].MaximumPacketSize

        laclient.subscribe(topics[0], qos=2)
        response = lacallback.wait_subscribed()

        # send a small enough packet, should get this one back
        payload = b"."*(int(maximumPacketSize/2))
        laclient.publish(topics[0], payload, 0)
        lacallback.messages.get(timeout=DEFAULT_TIMEOUT)

        # send a packet too big to receive
        payload = b"."*maximumPacketSize
        laclient.publish(topics[0], payload, 1)
        try:
            lacallback.messages.get(timeout=WAIT_NON_EVENT_TIMEOUT)
            raise ValueError("unexpected message received")
        except queue.Empty:
            pass

        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

    """
    def test_server_keep_alive(self):
        clientid = 'server keep alive'

        laclient, lacallback = self.new_client(clientid+" a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        connack = lacallback.wait_connected()
        laclient.loop_start()

        self.assertTrue(hasattr(connack["properties"], "ServerKeepAlive"))
        self.assertEqual(connack["properties"].ServerKeepAlive, 60)

        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()
    """

    def test_will_delay(self):
        # the will message should be received earlier than the session expiry

        clientid = 'will delay'

        will_properties = Properties(PacketTypes.WILLMESSAGE)
        connect_properties = Properties(PacketTypes.CONNECT)

        # set the will delay and session expiry to the same value -
        # then both should occur at the same time
        will_properties.WillDelayInterval = 3  # in seconds
        connect_properties.SessionExpiryInterval = 5

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.will_set(
            topics[0], payload=b"test_will_delay will message", properties=will_properties)
        laclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        connack = lacallback.wait_connected()
        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)
        laclient.loop_start()

        lbclient, lbcallback = self.new_client(f"{clientid} b")
        lbclient.connect(host="localhost", port=self._test_broker_port, properties=connect_properties)
        connack = lbcallback.wait_connected()
        lbclient.loop_start()
        # subscribe to will message topic
        lbclient.subscribe(topics[0], qos=2)
        lbcallback.wait_subscribed()

        # abort client a and wait for the will message
        laclient.loop_stop()
        laclient.socket().close()
        start = time.time()
        msg = lbcallback.messages.get(DEFAULT_TIMEOUT)
        duration = time.time() - start
        self.assertAlmostEqual(duration, 4, delta=1)
        self.assertEqual(msg["message"].topic, topics[0])
        self.assertEqual(
            msg["message"].payload, b"test_will_delay will message")

        lbclient.disconnect()
        lbcallback.wait_disconnected()
        lbclient.loop_stop()

    def test_shared_subscriptions(self):
        clientid = 'shared subscriptions'

        shared_sub_topic = f"$share/sharename/{topic_prefix}x"
        shared_pub_topic = f"{topic_prefix}x"

        laclient, lacallback = self.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=self._test_broker_port)
        connack = lacallback.wait_connected()
        laclient.loop_start()

        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)

        laclient.subscribe(
            [(shared_sub_topic, SubscribeOptions(2)), (topics[0], SubscribeOptions(2))])
        lacallback.wait_subscribed()

        lbclient, lbcallback = self.new_client(f"{clientid} b")
        lbclient.connect(host="localhost", port=self._test_broker_port)
        connack = lbcallback.wait_connected()
        lbclient.loop_start()

        self.assertEqual(connack["reasonCode"].getName(), "Success")
        self.assertEqual(connack["flags"]["session present"], False)

        lbclient.subscribe(
            [(shared_sub_topic, SubscribeOptions(2)), (topics[0], 2)])
        lbcallback.wait_subscribed()

        lacallback.clear()
        lbcallback.clear()

        count = 1
        for i in range(count):
            lbclient.publish(topics[0], f"message {i}", 0)

        lacallback.get_messages(count)
        lbcallback.get_messages(count)

        self.assertTrue(lacallback.messages.empty())
        self.assertTrue(lbcallback.messages.empty())

        lacallback.clear()
        lbcallback.clear()

        for i in range(count):
            lbclient.publish(shared_pub_topic, f"message {i}", 0)
        # Each message should only be received once
        result = []
        deadline = time.time() + DEFAULT_TIMEOUT
        while len(result) < count and time.time() < deadline:
            get_timeout = deadline - time.time()
            try:
                if get_timeout <= 0:
                    result.append(lacallback.messages.get_nowait())
                else:
                    result.append(lacallback.messages.get(timeout=get_timeout))
            except queue.Empty:
                # The message could be sent to other client, so empty queue
                # could be normal
                pass

            try:
                get_timeout = deadline - time.time()
                if get_timeout <= 0:
                    result.append(lbcallback.messages.get_nowait())
                else:
                    result.append(lbcallback.messages.get(timeout=get_timeout))
            except queue.Empty:
                # The message could be sent to other client, so empty queue
                # could be normal
                pass

        self.assertEqual(
            {x["message"].payload for x in result},
            {f"message {i}".encode() for i in range(count)}
        )

        laclient.disconnect()
        lacallback.wait_disconnected()
        laclient.loop_stop()

        lbclient.disconnect()
        lbcallback.wait_disconnected()
        lbclient.loop_stop()


class _TestBrokerRebootsTopicAliiMixin:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        clientid = f'client topic alias broker shutsdown QoS: {cls.qos}'

        laclient, lacallback = cls.new_client(f"{clientid} a")
        laclient.connect(host="localhost", port=cls._test_broker_port)
        _connack = lacallback.wait_connected()
        print("laclient Connected", flush=True)


        publish_properties = Properties(PacketTypes.PUBLISH)
        publish_properties.TopicAlias = 1

        topic = topics[0]
        laclient.subscribe(topic, qos=2)
        lacallback.wait_subscribed()

        laclient.publish(topic, b"topic alias 1",
                         cls.qos, properties=publish_properties)
        lacallback.messages.get(timeout=DEFAULT_TIMEOUT)

        for i in range(2,6):
            laclient.publish("", f"topic alias msg {i}".encode("ascii"), cls.qos,
                         properties=publish_properties)
            lacallback.messages.get(timeout=DEFAULT_TIMEOUT)

        # Simulate PUBACK being lost
        laclient._tmp_do_on_publish = laclient._do_on_publish
        with laclient._callback_mutex, laclient._in_callback_mutex, laclient._out_message_mutex:
            laclient._do_on_publish = lambda mid, rc, props : MQTTErrorCode.MQTT_ERR_SUCCESS



        msg_info = laclient.publish("", "topic alias msg 6".encode("ascii"), cls.qos,
                        properties=publish_properties)

        time.sleep(0.3)

        if cls.qos >= 1:
            # Assertion error will be ignored in setUp()
            laclient._out_messages[msg_info.mid]

        # Do not get last message...  ... in QoS >= 1, expect
        # this to causes client to get stuck in a loop:
        # i) reconnection
        # ii) publish QoS >= 1 message with no PUBACK to unrecognised alias
        # iii) Broker kicks client
        print("Rebooting broker...")
        cls._reboot_broker()

        lacallback.wait_disconnected()
        _connack = lacallback.wait_connected()

        cls.laclient, cls.lacallback = laclient, lacallback

    @classmethod
    def tearDownClass(cls):
        cls.laclient.loop_stop()
        super().tearDownClass()

class TestBrokerRebootsTopicAliiQoS0(_TestBrokerRebootsTopicAliiMixin, _TestBase):
    """ Simulates what happens when broker reboots on
        a QoS 0 client, that has had the code to clear the message
        queue on PUBACK deactivated.
    """
    qos=0
    def test_qos0_client_reconnects_after_broker_reboots(self):
        time.sleep(DEFAULT_TIMEOUT)
        self.assertTrue(self.lacallback.disconnecteds.empty())

class TestBrokerRebootsTopicAliiQoS1(_TestBrokerRebootsTopicAliiMixin, _TestBase):
    """ Simulated what happens when broker reboots on
        a QoS 1 client, that has had the code to clear the message
        queue on PUBACK deactivated.

        Note, on a similar class for QoS 2, the test setUpClass
        hangs or enters an infinite loop, causing the whole test
        session to time out.  So there is never
        anything for the xfail to test.
    """

    qos=1
    # xfail because we expect the new rebooted broker instance
    # to kick the client when it attempts to republish
    # an 'unacknowledged' message
    # on an old unrecognised topic alias.
    @unittest.expectedFailure
    def test_qos1_client_reconnects_after_broker_reboots(self):
        time.sleep(DEFAULT_TIMEOUT)
        self.assertTrue(self.lacallback.disconnecteds.empty())


class WorkAroundCallbacks(Callbacks):
    def on_connect(self, client, userdata, flags, reasonCode, properties):
        super().on_connect(client, userdata, flags, reasonCode, properties)
        # Users can write custom code to handle _dropped_messages,
        # e.g. save {alias: topic} somewhere and
        # restore full topic names to the first message to
        # each topic alias.
        _dropped_messages = client.drop_out_messages()


class TestDropOutMessagesWorkAround(_TestBrokerRebootsTopicAliiMixin, _TestBase):
    """ Tests simple work around (clearing pending messages)
        for the above QoS 1 / Topic alias  / broker reboot
        reconnection loop issue.
    """
    CallbacksClass = WorkAroundCallbacks
    qos=1
    def test_qos1_drop_messages_client_reconnects_after_broker_reboots(self):
        time.sleep(DEFAULT_TIMEOUT)
        self.assertTrue(self.lacallback.disconnecteds.empty())





def setData():
    global topics, wildtopics, nosubscribe_topics, topic_prefix
    topics = ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
    wildtopics = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")
    nosubscribe_topics = ("test/nosubscribe",)
    topic_prefix = "paho.mqtt.client.mqttv5/"
