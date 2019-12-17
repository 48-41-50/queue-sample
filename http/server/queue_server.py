#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http import HTTPStatus
import logging
from urllib.parse import urlparse, parse_qs, unquote_plus
import os
from psycopg2 import connect as db_connect
from psycopg2.extras import RealDictCursor
import json

from IPython import embed

DEFAULT_PORT=8888

DB_URI = os.environ.get('DB_URI', 'postgresql://queues@db:5432/queues')


class QRequestHandler(BaseHTTPRequestHandler):
    def _set_response(self, status=HTTPStatus.OK headers={'Content-type': 'text/json'}):
        self.send_response(status)
        for pair in headers.items():
            self.send_header(*pair)
        self.end_headers()
    
    def handle_route(self):
        parsed_path = urlparse(self.path)
        self._query_params = parse_qs(parsed_path.query)
        route = self._parsed_path.path.split('/')[-1:]
        route_method = 'do_' + route
        if not hasattr(self, route_method):
            self.send_error(
                HTTPStatus.NOT_IMPLEMENTED,
                f"Unsupported route ({route})")
            return
        getattr(self, route_method)()
    
    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self.handle_route()
    
    # create topic route: /topic?topic=<name>&description=<description>
    def do_topic(self):
        res = {}
        new_topic = self._query_params.get('topic')
        new_topic_desc = self._query_params.get('description')
        if new_topic and new_topic_desc:
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""select topic_create(%s, %s);""", (new_topic, new_topic_desc))
                    res = cur.fetchone()
            if res.get('topic_create') == new_topic:
                self._set_response()
                self.wfile.write(json.dumps({'topic': new_topic}))
            else:
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY)
        else;
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY)

    # delete topic route: /topic_delete?topic=<topic>
    def do_topic_delete(self):
        target_topic = self._query_params.get('topic')
        if target_topic:
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""call topic_delete(%s);""", (target_topic))
            self._set_response()
            self.wfile.write(json.dumps({'message': f'Topic "{target_topic} deleted'}))
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY)
    
    # save message route: /publish?topic=<topic>&message=<message> 
    def do_save_message(self):
        target_topic = self._query_params.get('topic')
        message = self._query_params.get('message')
        if target_topic and message is not None
            #message = unquote_plus(message)
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""call save_message(%s, %s);""", (target_topic, message))
            self._set_response()
            self.wfile.write(json.dumps({'message': f'Saved message to topic "{target_topic}'}))
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY)
    
    # subscribe route: /subscribe?topic=<topic>
    def do_subscribe(self):
        pass
    
    # unsubscribe route: /unsubscribe?subscriber_id=<id>
    def do_unsubscribe(self):
        pass
    
    # get message route: /get_message?topic=<topic>&subscriber_id=<subscriber_id>&num_messages=1
    def do_get_message(self):
        pass
    
    # ack_message route: /ack_message?subscriber_id=<subscriber_id>&offset=<offset>
    def do_ack_message(self):
        pass
        

def run(server_class=ThreadingHTTPServer, handler_class=QRequestHandler, port=DEFAULT_PORT):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting httpd...\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')

if __name__ == '__main__':
    from sys import argv
    
    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
    

