#! /usr/bin/env python3

import queue_publisher as qpub
import random
import sys
import time

random.seed(time.time())

topic = sys.argv[1]
max_messages = random.randrange(100, 10000)

create = len(sys.argv) == 3


qp = qpub.QPublisher()
if create:
    qp.create_topic(topic, f'testing messages with topic {topic}')

reset_count = 0
max_resets = 2
for mnum in range(1, max_messages):
    qp.publish_message(topic, f'test message {mnum}')
    if random.randrange(1, 100) > 95:
        reset_count += 1
        qp.reset_topic(topic, random.randrange(1, mnum))
