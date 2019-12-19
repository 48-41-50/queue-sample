#!/usr/bin/env python3

from http import HTTPStatus
import json
import logging
import os
import requests
import sys
import time

PUBLISHER_URL = os.environ.get('PUBLISHER_URL', 'http://queues-server:8888')


class ContentTypeError(Exception):
    pass


class UnsubscribedError(Exception):
    pass


class QSubscriber:
    def __init__(self, topic: str, sleep_time: int, exit_after: int):
        self.topic = topic
        self.sleep_time = sleep_time
        self.exit_after = exit_after
        self.subscribe()
        self.processed = set()
        filename = f'{self.topic}_subscriber_{self.subscriber_id}.log'
        logging.basicConfig(filename=filename,
                            filemode='w',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        self._log = logging.getLogger(filename)
    
    def _handle_request(self, url: str, data: dict, request_type: str='get'):
        rmethod = getattr(requests, request_type)
        if request_type == 'post':
            rargs = {'json': data}
        else:
            rargs = {'params': data}
        res = rmethod(url, **rargs)
        res.raise_for_status()
        
        if res.status_code == HTTPStatus.OK:
            if res.headers and 'Content-type' in res.headers:
                if res.headers['Content-type'] == 'application/json':
                    return res.json()
                else:
                    raise ContentTypeError(f'Expected "application/json" but got {res.headers["Content-type"]}')

    def subscribe(self):
        data = self._handle_request(f'{PUBLISHER_URL}/subscribe', {'topic': self.topic})
        if data:
            self.subscriber_id = data['subscriber_id']
    
    def unsubscribe(self):
        data = self._handle_request(f'{PUBLISHER_URL}/unsubscribe', {'subscriber_id': self.subscriber_id}, 'post')
    
    def ack_message(self, _offset: int):
        data = self._handle_request(f'{PUBLISHER_URL}/ack_message', {'subscriber_id': self.subscriber_id, 'offset': _offset}, 'post')

    def get_message(self):
        # candidate for async
        if not self.subscriber_id:
            raise UnsubscribedError(f"Not subscribed to topic {self.topic}")
        
        return self._handle_request(f'{PUBLISHER_URL}/get_message', {'subscriber_id': self.subscriber_id})
    
    def process_message(self, message_data: dict):
        if message_data['_offset'] not in self.processed:
            self.processed.add(message_data['_offset'])
            label = 'Processed'
        else:
            label = 'Re-Processed'
        
        self._log.info(f'{label} {message_data["_offset"]: 5d} : "{message_data["message"]}"{os.linesep}')
        self.ack_message(message_data['_offset'])
    
    def consume(self):
        """
        Loop to consume messages, sleep between calls by a specified amount and unsubscribe and exit after a certain number of detected empty responses
        """
        tries = 0
        try:
            while tries < self.exit_after:
                data = self.get_message()
                if data:
                    tries = 0
                    # candidate for async
                    self.process_message(data)
                else:
                    tries += 1
                
                # candidate for async
                time.sleep(self.sleep_time)
        finally:
            self.unsubscribe()
        
            
if __name__ == '__main__':
    if len(sys.argv) == 1:
        print(f"""
Usage: {os.path.basename(sys.argv[0])} <topic-name> <sleep-time-in-seconds> <exit-after-tries>
""", file=sys.stderr)
        sys.exit(1)
    else:
        topic = sys.argv[1]
        sleep_time = int((sys.argv[2:3] or [1])[0])
        exit_after = int((sys.argv[3:4] or [10])[0])
        QSubscriber(topic, sleep_time, exit_after).consume()
        
    
    
    
    
