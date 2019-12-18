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
import psycopg2.errors
import json

from IPython import embed

DEFAULT_PORT=8888

DB_URI = os.environ.get('DB_URI', 'postgresql://queues@db:5432/queues')


class QRequestHandler(BaseHTTPRequestHandler):
    def _set_response(self, status=HTTPStatus.OK, headers={'Content-type': 'text/json'}):
        self.send_response(status)
        for pair in headers.items():
            self.send_header(*pair)
        self.end_headers()
    
    def handle_route(self):
        parsed_path = urlparse(self.path)
        self._query_params = parse_qs(parsed_path.query)
        route = parsed_path.path.split('/')[-1:]
        if route:
            route = route[0]
        else:
            route = '___no_method_here'
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
        new_topic = self._query_params.get('topic')
        new_topic_desc = self._query_params.get('description')
        if new_topic and new_topic_desc:
            new_topic = new_topic[0]
            new_topic_desc = new_topic_desc[0]
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select topic_create(%s, %s);""", (new_topic, new_topic_desc))
                        res = cur.fetchone()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                logging.error(f'Topic "{new_topic}" already exists')
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, f'The topic "{new_topic}" already exists')
                return

            if res.get('topic_create') == new_topic:
                logging.info(f"Created topic '{new_topic}'")
                self._set_response()
                self.wfile.write(json.dumps({'message': f'Topic "{new_topic}" created'}).encode('utf-8'))
            else:
                logging.error(f"ERROR: Topic '{new_topic}' creation failed")
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY)
        else:
            logging.error("ERROR: /topic route missing parameters for topic or description")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, "Missing parameters topic, description")

    # delete topic route: /topic_delete?topic=<topic>
    def do_topic_delete(self):
        target_topic = self._query_params.get('topic')
        if target_topic:
            target_topic = target_topic[0]
            logging.info(f"Deleting topic {target_topic}")
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""select topic_delete(%s);""", (target_topic,))
            self._set_response()
            self.wfile.write(json.dumps({'message': f'Topic "{target_topic}" deleted'}).encode('utf-8'))
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, "Missing parameter: topic")
    
    # save message route: /publish?topic=<topic>&message=<message> 
    def do_publish(self):
        target_topic = self._query_params.get('topic')
        message = self._query_params.get('message')
        if target_topic and message is not None:
            target_topic = target_topic[0]
            message = message[0]
            logging.info(f'MESSAGE = [{message}]')
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select save_message(%s, %s);""", (target_topic, message))
                        res = cur.fetchone()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                logging.error(f'Topic "{target_topic}" does not exist')
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, f'Topic "{target_topic}" does not exist')
                return
                        
            if res is not None and res['save_message'] is not None:
                logging.info(f'Saved message for topic {target_topic} at offset {res["save_message"]}')
                self._set_response()
                self.wfile.write(json.dumps({'message': f'Saved message to topic "{target_topic}" at offset {res["save_message"]}'}).encode('utf-8'))
            else:
                err_msg = f"Failed to save message to topic {target_topic}"
                logging.error(err_msg)
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, err_msg)
        else:
            err_msg = "Missing parameters topic, message"
            logging.error(err_msg)
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, err_msg)
    
    # subscribe route: /subscribe?topic=<topic>
    def do_subscribe(self):
        target_topic = self._query_params.get('topic')
        if target_topic:
            target_topic = target_topic[0]
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select topic_subscribe(%s);""", (target_topic,))
                        res = cur.fetchone()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                err_msg = f'The topic "{target_topic}" does not exist'
                logging.error(err_msg)
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, err_msg)
                return
            
            self._set_response()
            self.wfile.write(json.dumps({'topic': target_topic, 'subscriber_id': res['topic_subscribe']}).encode('utf-8'))
        else:
            logging.error("ERROR: /topic route missing parameters for topic")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, "missing 'topic' parameter")
    
    # unsubscribe route: /unsubscribe?subscriber_id=<id>
    def do_unsubscribe(self):
        target_subscriber_id = self._query_params.get('subscriber_id')
        if target_subscriber_id:
            try:
                target_subscriber_id = int(target_subscriber_id[0])
            except ValueError:
                err_msg = f'Expected integer value for the subscriber_id parameter. Got [{target_subscriber_id[0]}].'
                logging.error(err_msg)
                send_error(HTTPStatus.UNPROCESSABLE_ENTITY, err_msg)
                return
                
            logging.info(f"Deleting subscriber {target_subscriber_id}")
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""select unsubscribe(%s);""", (target_subscriber_id,))
            self._set_response()
            self.wfile.write(json.dumps({'message': f'Subscriber "{target_subscriber_id}" deleted'}).encode('utf-8'))
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, "Missing parameter: subscriber_id")
    
    # get message route: /get_message?subscriber_id=<subscriber_id>&num_messages=1
    def do_get_message(self):
        subscriber_id = self._query_params.get('subscriber_id')
        num_messages = self._query_params.get('num_messages', [1])
        err_msg = []
        if subscriber_id and num_messages:
            try:
                subscriber_id = int(subscriber_id[0])
            except ValueError:
                err_msg.append(f'Expected integer value for the subscriber_id parameter. Got [{subscriber_id[0]}].')
            try:
                num_messages = int(num_messages[0])
            except ValueError:
                err_msg.append(f'Expected integer value for the num_messages parameter. Got [{num_messages[0]}].')
            if err_msg:
                logging.error('\n'.join(err_msg))
                send_error(HTTPStatus.UNPROCESSABLE_ENTITY, '<br>'.join(err_msg))
                return
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select * from get_message(%s, %s);""", (subscriber_id,num_messages))
                        res = cur.fetchall()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                err_msg = f'The topic "{new_topic}" does not exist'
                logging.error(err_msg)
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, err_msg)
                return
            
            logging.info(f'Fetched {len(res)} messages for subscriber {subscriber_id}')
            if len(res) == 1:
                res = res[0]
            self._set_response()
            self.wfile.write(json.dumps(res, default=str).encode('utf-8'))
        else:
            logging.error("ERROR: /topic route missing parameters for topic")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, "missing 'topic' parameter")
    
    # ack_message route: /ack_message?subscriber_id=<subscriber_id>&offset=<offset>
    def do_ack_message(self):
        subscriber_id = self._query_params.get('subscriber_id')
        offset = self._query_params.get('offset')
        err_msg = []
        if subscriber_id and offset:
            try:
                subscriber_id = int(subscriber_id[0])
            except ValueError:
                err_msg.append(f'Expected integer value for the subscriber_id parameter. Got [{subscriber_id[0]}].')
            try:
                offset = int(offset[0])
            except ValueError:
                err_msg.append(f'Expected integer value for the offset parameter. Got [{offset[0]}].')
            if err_msg:
                logging.error('\n'.join(err_msg))
                send_error(HTTPStatus.UNPROCESSABLE_ENTITY, '<br>'.join(err_msg))
                return
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""select * from ack_message(%s, %s);""", (subscriber_id, offset))
                    res = cur.fetchone()
            self._set_response()
            self.wfile.write(json.dumps(res, default=str).encode('utf-8'))
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, "Missing parameter: topic")

    # topic_reset route: /topic_reset?topic=<topic>&offset=<offset>
    def do_topic_reset(self):
        target_topic = self._query_params.get('topic')
        offset = self._query_params.get('offset', [1])
        if target_topic and offset:
            target_topic = target_topic[0]
            try:
                offset = int(offset[0])
            except ValueError:
                err_msg = f'Expected integer value for the subscriber_id parameter. Got [{offset[0]}].'
                logging.error(err_msg)
                send_error(HTTPStatus.UNPROCESSABLE_ENTITY, err_msg)
                return
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select topic_reset(%s, %s);""", (target_topic, offset - 1))
                        res = cur.fetchone()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                err_msg = f'The topic "{new_topic}" does not exist'
                logging.error(err_msg)
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, err_msg)
                return
            
            logging.info(f'Set {res["topic_reset"]} subscribers of topic {target_topic} to reprocess any messages starting at offset {offset}')
            self._set_response()
            self.wfile.write(json.dumps({'topic': target_topic, 'offset': offset, 'subscriber_count': res['topic_reset']}, default=str).encode('utf-8'))
        else:
            logging.error("ERROR: /topic route missing parameters for topic")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, "missing 'topic' parameter")
        
        

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
    

