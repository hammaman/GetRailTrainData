#!/usr/bin/env python3

# Standard
import argparse
import json
from time import sleep

# Third party
import stomp

# Internal
from modules.classListener import Listener

requiredberths = ["0107", "0109", "0113", "0115", "0117", "0106", "0108", "0110", "0114", "0116", "0118"]
requiredareas = ["CA"]
requiredtrainids = []
durablesubscription = False

with open("secrets.json") as f:
    feed_username, feed_password = json.load(f)

# https://stomp.github.io/stomp-specification-1.2.html#Heart-beating
# We're committing to sending and accepting heartbeats every 5000ms
connection = stomp.Connection([('publicdatafeeds.networkrail.co.uk', 61618)], keepalive=True, heartbeats=(5000, 5000))
connection.set_listener('', Listener(connection, berths=requiredberths, areas=requiredareas, trainids=requiredtrainids))

# Connect to feed
connect_headers = {
    "username": feed_username,
    "passcode": feed_password,
    "wait": True,
    }

if durablesubscription:
    # The client-id header is part of the durable subscription - it should be unique to your account
    connect_headers["client-id"] = feed_username

connection.connect(**connect_headers)

# Determine topic to subscribe
topic = "/topic/TD_ALL_SIG_AREA"

# Subscription
subscribe_headers = {
    "destination": topic,
    "id": 1,
}

if durablesubscription:
    # Note that the subscription name should be unique both per connection and per queue
    subscribe_headers.update({
        "activemq.subscriptionName": feed_username + topic,
        "ack": "client-individual"
        })
else:
    subscribe_headers["ack"] = "auto"

connection.subscribe(**subscribe_headers)

while connection.is_connected():
    sleep(1)
