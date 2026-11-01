#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Copyright (c) 2013 Roger Light <roger@atchoo.org>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Distribution License v1.0
# which accompanies this distribution.
#
# The Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# Contributors:
#    Roger Light - initial implementation
# Copyright (c) 2010,2011 Roger Light <roger@atchoo.org>
# All rights reserved.

# This shows an example of an MQTT client that clears all of the retained messages it receives.

import getopt
import sys

import context  # Ensures paho is in PYTHONPATH

import paho.mqtt.client as mqtt

final_mid = 0


def on_connect(mqttc, userdata, flags, reason_code, properties):
    if userdata:
        print(f"reason_code: {reason_code}")


def on_message(mqttc, userdata, msg):
    global final_mid
    if msg.retain == 0:
        pass
        # sys.exit()
    else:
        if userdata:
            print("Clearing topic " + msg.topic)
        (_rc, final_mid) = mqttc.publish(msg.topic, None, 1, True)


def on_publish(mqttc, userdata, mid, reason_code, properties):
    if mid == final_mid:
        sys.exit()


def on_log(mqttc, userdata, level, string):
    print(string)


def print_usage():
    print(
        "mqtt_clear_retain.py [-d] [-h hostname] [-i clientid] [-k keepalive] [-p port] [-u username [-P password]] [-v] -t topic")


def main(argv):
    debug = False
    host = "mqtt.eclipseprojects.io"
    client_id = None
    keepalive = 60
    port = 1883
    password = None
    topic = None
    username = None
    verbose = False

    try:
        opts, _args = getopt.getopt(argv, "dh:i:k:p:P:t:u:v",
                                   ["debug", "id", "keepalive", "port", "password", "topic", "username", "verbose"])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--debug"):
            debug = True
        elif opt in ("-h", "--host"):
            host = arg
        elif opt in ("-i", "--id"):
            client_id = arg
        elif opt in ("-k", "--keepalive"):
            keepalive = int(arg)
        elif opt in ("-p", "--port"):
            port = int(arg)
        elif opt in ("-P", "--password"):
            password = arg
        elif opt in ("-t", "--topic"):
            topic = arg
            print(topic)
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-v", "--verbose"):
            verbose = True

    if not topic:
        print("You must provide a topic to clear.\n")
        print_usage()
        sys.exit(2)

    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id)
    mqttc._userdata = verbose
    mqttc.on_message = on_message
    mqttc.on_publish = on_publish
    mqttc.on_connect = on_connect
    if debug:
        mqttc.on_log = on_log

    if username:
        mqttc.username_pw_set(username, password)
    mqttc.connect(host, port, keepalive)
    mqttc.subscribe(topic)
    mqttc.loop_forever()


if __name__ == "__main__":
    main(sys.argv[1:])
