#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Copyright (c) 2017 Jon Levell <levell@uk.ibm.com>
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Distribution License v1.0
# which accompanies this distribution.
#
# The Eclipse Distribution License is available at
#   http://www.eclipse.org/org/documents/edl-v10.php.
#
# All rights reserved.

# This shows a example of an MQTT publisher with the ability to use
# user name, password CA certificates based on command line arguments

import argparse
import os
import ssl
import time

import paho.mqtt.client as mqtt

parser = argparse.ArgumentParser()

parser.add_argument('-H', '--host', required=False, default="mqtt.eclipseprojects.io")
parser.add_argument('-t', '--topic', required=False, default="paho/test/opts")
parser.add_argument('-q', '--qos', required=False, type=int,default=0)
parser.add_argument('-c', '--clientid', required=False, default=None)
parser.add_argument('-u', '--username', required=False, default=None)
parser.add_argument('-d', '--disable-clean-session', action='store_true', help="disable 'clean session' (sub + msgs not cleared when client disconnects)")
parser.add_argument('-p', '--password', required=False, default=None)
parser.add_argument('-P', '--port', required=False, type=int, default=None, help='Defaults to 8883 for TLS or 1883 for non-TLS')
parser.add_argument('-N', '--nummsgs', required=False, type=int, default=1, help='send this many messages before disconnecting')
parser.add_argument('-S', '--delay', required=False, type=float, default=1, help='number of seconds to sleep between msgs')
parser.add_argument('-k', '--keepalive', required=False, type=int, default=60)
parser.add_argument('-s', '--use-tls', action='store_true')
parser.add_argument('--insecure', action='store_true')
parser.add_argument('-F', '--cacerts', required=False, default=None)
parser.add_argument('--tls-version', required=False, default=None, help='TLS protocol version, can be one of tlsv1.2 tlsv1.1 or tlsv1\n')
parser.add_argument('-D', '--debug', action='store_true')

args, unknown = parser.parse_known_args()


def on_connect(mqttc, obj, flags, reason_code, properties):
    print("connect reason_code: " + str(reason_code))


def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def on_publish(mqttc, obj, mid, reason_code, properties):
    print("mid: " + str(mid))


def on_log(mqttc, obj, level, string):
    print(string)

usetls = args.use_tls

if args.cacerts:
    usetls = True

port = args.port
if port is None:
    if usetls:
        port = 8883
    else:
        port = 1883

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, args.clientid, clean_session = not args.disable_clean_session)

if usetls:
    if args.tls_version == "tlsv1.2":
       tlsVersion = ssl.PROTOCOL_TLSv1_2
    elif args.tls_version == "tlsv1.1":
       tlsVersion = ssl.PROTOCOL_TLSv1_1
    elif args.tls_version == "tlsv1":
       tlsVersion = ssl.PROTOCOL_TLSv1
    elif args.tls_version is None:
       tlsVersion = None
    else:
       print ("Unknown TLS version - ignoring")
       tlsVersion = None

    if not args.insecure:
        cert_required = ssl.CERT_REQUIRED
    else:
        cert_required = ssl.CERT_NONE

    mqttc.tls_set(ca_certs=args.cacerts, certfile=None, keyfile=None, cert_reqs=cert_required, tls_version=tlsVersion)

    if args.insecure:
        mqttc.tls_insecure_set(True)

if args.username or args.password:
    mqttc.username_pw_set(args.username, args.password)

mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish

if args.debug:
    mqttc.on_log = on_log

print("Connecting to "+args.host+" port: "+str(port))
mqttc.connect(args.host, port, args.keepalive)

mqttc.loop_start()

for x in range(args.nummsgs):
    msg_txt = '{"msgnum": "'+str(x)+'"}'
    print("Publishing: "+msg_txt)
    infot = mqttc.publish(args.topic, msg_txt, qos=args.qos)
    infot.wait_for_publish()

    time.sleep(args.delay)

mqttc.disconnect()

