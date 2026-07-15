import paho.mqtt.client as client
from paho.mqtt.enums import CallbackAPIVersion


def test_message_topic_string_is_cached_and_invalidated():
    message = client.MQTTMessage(create_info=False)
    message.topic = b"sensors/1"

    assert message._topic_str is None
    assert message.topic == "sensors/1"
    assert message._topic_str == "sensors/1"
    assert message.topic == "sensors/1"

    message.topic = b"sensors/2"
    assert message._topic_str is None
    assert message.topic == "sensors/2"


def test_incoming_message_info_is_created_lazily():
    message = client.MQTTMessage(create_info=False)

    assert message._info is None

    info = message.info

    assert isinstance(info, client.MQTTMessageInfo)
    assert message.info is info


def test_filtered_callback_count_tracks_add_replace_remove():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)

    def callback_one(mqttc, userdata, message):
        pass

    def callback_two(mqttc, userdata, message):
        pass

    assert mqttc._on_message_filtered_count == 0

    mqttc.message_callback_add("sensors/+", callback_one)
    assert mqttc._on_message_filtered_count == 1

    mqttc.message_callback_add("sensors/+", callback_two)
    assert mqttc._on_message_filtered_count == 1

    mqttc.message_callback_add("devices/#", callback_one)
    assert mqttc._on_message_filtered_count == 2

    mqttc.message_callback_remove("missing/#")
    assert mqttc._on_message_filtered_count == 2

    mqttc.message_callback_remove("sensors/+")
    assert mqttc._on_message_filtered_count == 1

    mqttc.message_callback_remove("devices/#")
    assert mqttc._on_message_filtered_count == 0


def test_unfiltered_on_message_does_not_require_valid_utf8_topic():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    messages = []
    mqttc.on_message = lambda mqttc, userdata, message: messages.append(message)

    message = client.MQTTMessage(create_info=False)
    message.topic = b"\xff"

    mqttc._handle_on_message(message)

    assert messages == [message]


def test_multiple_filtered_callbacks_are_invoked():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    seen = []
    mqttc.on_message = lambda *args: seen.append("global")
    mqttc.message_callback_add("sensors/+", lambda *args: seen.append("plus"))
    mqttc.message_callback_add("sensors/#", lambda *args: seen.append("hash"))

    message = client.MQTTMessage(create_info=False)
    message.topic = b"sensors/1"
    mqttc._handle_on_message(message)

    assert seen == ["plus", "hash"]


def test_callback_remove_during_dispatch_does_not_skip_snapshot():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    seen = []

    def first(mqttc, userdata, message):
        seen.append("first")
        mqttc.message_callback_remove("sensors/#")

    def second(mqttc, userdata, message):
        seen.append("second")

    mqttc.message_callback_add("sensors/+", first)
    mqttc.message_callback_add("sensors/#", second)

    message = client.MQTTMessage(create_info=False)
    message.topic = b"sensors/1"
    mqttc._handle_on_message(message)

    assert seen == ["first", "second"]
    assert mqttc._on_message_filtered_count == 1


def test_callback_add_during_dispatch_is_not_invoked_same_message():
    mqttc = client.Client(callback_api_version=CallbackAPIVersion.VERSION2)
    seen = []

    def late(mqttc, userdata, message):
        seen.append("late")

    def first(mqttc, userdata, message):
        seen.append("first")
        mqttc.message_callback_add("sensors/#", late)

    mqttc.message_callback_add("sensors/+", first)

    message = client.MQTTMessage(create_info=False)
    message.topic = b"sensors/1"
    mqttc._handle_on_message(message)

    assert seen == ["first"]
    assert mqttc._on_message_filtered_count == 2

    seen.clear()
    mqttc._handle_on_message(message)
    assert seen == ["first", "late"]
