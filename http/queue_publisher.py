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


class QPublisher:
    def __init__(self, log_to_file=True):
        self.id = time.time()
        
        logging_params = {'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                          'level': logging.INFO}
        if log_to_file:
            logging_params['filename'] = f'publisher_{self.id}.log'
            logging_params['filemode'] = 'w'
        else:
            logging_params['stream'] = sys.stderr
        
        logging.basicConfig(**logging_params)
        self._log = logging.getLogger(__file__)
    
    def _handle_request(self, url: str, data: dict, request_type: str='post'):
        rmethod = getattr(requests, request_type)
        if request_type == 'post':
            rargs = {'json': data}
        else:
            rargs = {'params': data}
        res = rmethod(url, **rargs)
        res.raise_for_status()
        
        if res.status_code == HTTPStatus.OK:
            if 'Content-type' in res.headers:
                if res.headers['Content-type'] == 'application/json':
                    return res.json()
                else:
                    raise ContentTypeError(f'Expected "application/json" but got {res.headers["Content-type"]}')

    def create_topic(self, topic: str, description: str):
        data = self._handle_request(f'{PUBLISHER_URL}/topic', {'topic': topic, 'description': description})
        self._log.info(f"Topic {data['topic']} created")
    
    def delete_topic(self, topic: str):
        data = self._handle_request(f'{PUBLISHER_URL}/topic_delete', {'topic': topic})
    
    def reset_topic(self, topic: str, offset: int):
        data = self._handle_request(f'{PUBLISHER_URL}/topic_reset', {'topic': topic, 'offset': offset})
    
    def list_topics(self, topic: str=''):
        if topic:
            topic_param = {'topic': topic}
        else:
            topic_param = {}
        data = self._handle_request(f'{PUBLISHER_URL}/topics', topic_param, 'get')
        self._log.info(data)
        
    def publish_message(self, topic: str, message: str):
        data = self._handle_request(f'{PUBLISHER_URL}/publish', {'topic': topic, 'message': message})

    def list_messages(self, topic: str=''):
        if topic:
            topic_param = {'topic': topic}
        else:
            topic_param = {}
        data = self._handle_request(f'{PUBLISHER_URL}/topic_messages', topic_param, 'get')
        self._log.info(data)
        
    def list_subscribers(self, topic: str=''):
        if topic:
            topic_param = {'topic': topic}
        else:
            topic_param = {}
        data = self._handle_request(f'{PUBLISHER_URL}/topic_subscribers', topic_param, 'get')
        self._log.info(data)
        
        
            
if __name__ == '__main__':
    qp = QPublisher()
    qp.create_topic('search-web')
        
    
    
    
    
