#!/usr/bin/python3
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Roger Light <roger@atchoo.org>
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

# This shows an example of using the subscribe.callback helper function.

import context  # Ensures paho is in PYTHONPATH

from paho.mqtt import subscribe


def print_msg(client, userdata, message):
    print("%s : %s" % (message.topic, message.payload))

subscribe.callback(print_msg, "#", hostname="mqtt.eclipseprojects.io")
