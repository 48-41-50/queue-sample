#! /usr/bin/env python3

import queue_subscriber as qsub
import sys

topic = sys.argv[1]

qs = qsub.QSubscriber(topic, 0, 10)
qs.consume()


    
    
