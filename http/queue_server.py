#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]

This is code taken from https://gist.github.com/mdonkers/63e115cc0c79b4f6b8b3a6b797e485c7
and modified. This was chosen for simplicity in order to complete the project requirements
in a timely manner.
"""
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from psycopg2 import connect as db_connect
from psycopg2.extras import RealDictCursor
import psycopg2.errors
from urllib.parse import urlparse, parse_qs, unquote_plus

from IPython import embed

DEFAULT_PORT=8888

# default is to use the docker networking from docker-compose
DB_URI = os.environ.get('DB_URI', 'postgresql://queues@db:5432/queues')


# The server request handler
class QRequestHandler(BaseHTTPRequestHandler):
    def _set_response(self, status: int=HTTPStatus.OK, headers: dict={'Content-type': 'application/json'}):
        self.send_response(status)
        for pair in headers.items():
            self.send_header(*pair)
        self.end_headers()
    
    def send_error(self, status:int, data: dict={}):
        kwargs = {'status': status}
        if not data:
            kwargs['headers'] = {}
        self._set_response(**kwargs)
        if data:
            self.wfile.write(json.dumps(data, default=str).encode('utf-8'))
    
    def handle_route(self, allowed_routes=[]):
        parsed_path = urlparse(self.path)
        
        if self.command.upper() == 'POST':
            varlen = int(self.headers['Content-Length'])
            if self.headers['Content-type'] == 'application/json':
                self._query_params = json.loads(self.rfile.read(varlen).decode('utf-8'))
            else:
                content_type = self.headers['Content-type']
                self.send_error(
                    HTTPStatus.BAD_REQUEST,
                    {'http_status': HTTPStatus.BAD_REQUEST, 'error_message': f'POST command only accepts "application/json" content type. (Got "{content_type}")'} )
        else:
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
                {'http_status': HTTPStatus.NOT_IMPLEMENTED, 'error_message': f"Unsupported route ({route})"} )
            return
        
        if not allowed_routes or route in allowed_routes:
            getattr(self, route_method)()
        else:
            err_msg = f"Unsupported route ({route}) for {self.command}"
            logging.error(err_msg)
            self.send_error(
                HTTPStatus.UNAUTHORIZED,
                {'http_status': HTTPStatus.UNAUTHORIZED, 'error_message': err_msg})
    
    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self.handle_route()
    
    def do_POST(self):
        logging.info("POST request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self.handle_route(allowed_routes=['publish', 'ack_message', 'topic_reset', 'unsubscribe', 'topic'])
    
    def do_PUT(self):
        logging.info("PUT request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self.handle_route(allowed_routes=['publish', 'ack_message', 'topic_reset', 'unsubscribe', 'topic'])
    
    def do_PATCH(self):
        self.send_error(
            HTTPStatus.NOT_IMPLEMENTED,
            {'http_status': HTTPStatus.NOT_IMPLEMENTED, 'error_message': f"Unsupported command"})
    
    def do_DELETE(self):
        logging.info("DELETE request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self.handle_route(allowed_routes=['topic_delete', 'unsubscribe'])
    
    def _do_list(self, table_name: str):
        sql = f"""select * from {table_name} """
        target_topic = self._query_params.get('topic')
        if target_topic:
            target_topic = target_topic[0] if isinstance(target_topic, (list, tuple)) else target_topic
        if target_topic:
            values = (target_topic,)
            sql += """where topic = %s"""
        else:
            values = None
        sql = sql + ';'
        
        with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, values)
                res = cur.fetchall()
        self._set_response()
        logging.info(f'Got {len(res)} records from table {table_name}')
        self.wfile.write(json.dumps(res, default=str).encode('utf-8'))
    
    # list topics route: /topics
    def do_topics(self):
        self._do_list("topic")
    
    # list messages route: /topic_messages?topic=<topic>
    def do_topic_messages(self):
        self._do_list("topic_message")

    # list subscribers route: /topic_subscribers?topic=<topic>
    def do_topic_subscribers(self):
        self._do_list("topic_subscriber")

    # create topic route: /topic?topic=<name>&description=<description>
    def do_topic(self):
        new_topic = self._query_params.get('topic')
        new_topic_desc = self._query_params.get('description')
        if new_topic and new_topic_desc:
            new_topic = new_topic[0] if isinstance(new_topic, (list, tuple)) else new_topic
            new_topic_desc = new_topic_desc[0] if isinstance(new_topic_desc, (list, tuple)) else new_topic_desc
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select topic_create(%s, %s);""", (new_topic, new_topic_desc))
                        res = cur.fetchone()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                logging.error(f'Topic "{new_topic}" already exists')
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                                {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': f'The topic "{new_topic}" already exists'})
                return

            if res.get('topic_create') == new_topic:
                logging.info(f"Created topic '{new_topic}'")
                self._set_response()
                self.wfile.write(json.dumps({'topic': new_topic, 'message': f'Topic "{new_topic}" created'}).encode('utf-8'))
            else:
                logging.error(f"ERROR: Topic '{new_topic}' creation failed")
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY,
                                {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': f'Unknown error -- topic {new_topic} creation failed'})
        else:
            logging.error("ERROR: /topic route missing parameters for topic or description")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': "Missing parameters topic, description"})

    # delete topic route: /topic_delete?topic=<topic>
    def do_topic_delete(self):
        target_topic = self._query_params.get('topic')
        if target_topic:
            target_topic = target_topic[0] if isinstance(target_topic, (list, tuple)) else target_topic
            logging.info(f"Deleting topic {target_topic}")
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""select topic_delete(%s);""", (target_topic,))
            if self.command == 'GET':
                self._set_response()
                self.wfile.write(json.dumps({'message': f'Topic "{target_topic}" deleted'}).encode('utf-8'))
            else:
                self._set_response(headers={})
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': "Missing parameter: topic"})
    
    # save message route: /publish?topic=<topic>&message=<message> 
    def do_publish(self):
        target_topic = self._query_params.get('topic')
        message = self._query_params.get('message')
        if target_topic and message is not None:
            target_topic = target_topic[0] if isinstance(target_topic, (list, tuple)) else target_topic
            message = message[0] if isinstance(message, (list, tuple)) else message
            logging.info(f'MESSAGE = [{message}]')
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select save_message(%s, %s);""", (target_topic, message))
                        res = cur.fetchone()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                logging.error(f'Topic "{target_topic}" does not exist')
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                                {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': f'Topic "{target_topic}" does not exist'})
                return
                        
            if res is not None and res['save_message'] is not None:
                logging.info(f'Saved message for topic {target_topic} at offset {res["save_message"]}')
                if self.command == 'GET':
                    self._set_response()
                    self.wfile.write(json.dumps({'message': f'Saved message to topic "{target_topic}" at offset {res["save_message"]}'}).encode('utf-8'))
                else:
                    self._set_response(headers={})
            else:
                err_msg = f"Failed to save message to topic {target_topic}"
                logging.error(err_msg)
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                                {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': err_msg})
        else:
            err_msg = "Missing parameters topic, message"
            logging.error(err_msg)
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': err_msg})
    
    # subscribe route: /subscribe?topic=<topic>
    def do_subscribe(self):
        target_topic = self._query_params.get('topic')
        if target_topic:
            target_topic = target_topic[0] if isinstance(target_topic, (list, tuple)) else target_topic
            try:
                with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select topic_subscribe(%s);""", (target_topic,))
                        res = cur.fetchone()
            except psycopg2.errors.IntegrityError as e:
                conn.rollback()
                err_msg = f'The topic "{target_topic}" does not exist'
                logging.error(err_msg)
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                                {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': err_msg})
                return
            
            self._set_response()
            self.wfile.write(json.dumps({'topic': target_topic, 'subscriber_id': res['topic_subscribe']}).encode('utf-8'))
        else:
            logging.error("ERROR: /topic route missing parameters for topic")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': "Missing 'topic' parameter"})
    
    # unsubscribe route: /unsubscribe?subscriber_id=<id>
    def do_unsubscribe(self):
        target_subscriber_id = self._query_params.get('subscriber_id')
        if target_subscriber_id:
            try:
                target_subscriber_id = int(target_subscriber_id[0] if isinstance(target_subscriber_id, (list, tuple)) else target_subscriber_id)
            except ValueError:
                err_msg = f'Expected integer value for the subscriber_id parameter. Got [{target_subscriber_id}].'
                logging.error(err_msg)
                send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                           {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': err_msg})
                return
                
            logging.info(f"Deleting subscriber {target_subscriber_id}")
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""select unsubscribe(%s);""", (target_subscriber_id,))
            if self.command == 'GET':
                self._set_response()
                self.wfile.write(json.dumps({'message': f'Subscriber "{target_subscriber_id}" deleted'}).encode('utf-8'))
            else:
                self._set_response(headers={})
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': "Missing parameter: subscriber_id"})
    
    # get message route: /get_message?subscriber_id=<subscriber_id>&num_messages=1
    def do_get_message(self):
        subscriber_id = self._query_params.get('subscriber_id')
        num_messages = self._query_params.get('num_messages', [1])
        err_msg = []
        if subscriber_id and num_messages:
            try:
                subscriber_id = int(subscriber_id[0] if isinstance(subscriber_id, (list, tuple)) else subscriber_id)
            except ValueError:
                err_msg.append(f'Expected integer value for the subscriber_id parameter. Got [{subscriber_id}].')
            try:
                num_messages = int(num_messages[0] if isinstance(num_messages, (list, tuple)) else num_messages)
            except ValueError:
                err_msg.append(f'Expected integer value for the num_messages parameter. Got [{num_messages}].')
            if err_msg:
                logging.error('\n'.join(err_msg))
                send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                           {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': '<br>'.join(err_msg)})
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
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                                {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': err_msg})
                return
            
            logging.info(f'Fetched {len(res)} messages for subscriber {subscriber_id}')
            if len(res) == 1:
                res = res[0]
            self._set_response()
            self.wfile.write(json.dumps(res, default=str).encode('utf-8'))
        else:
            logging.error("ERROR: /topic route missing parameters for topic")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': "Missing 'topic' parameter"})
    
    # ack_message route: /ack_message?subscriber_id=<subscriber_id>&offset=<offset>
    def do_ack_message(self):
        subscriber_id = self._query_params.get('subscriber_id')
        offset = self._query_params.get('offset')
        err_msg = []
        if subscriber_id and offset:
            try:
                subscriber_id = int(subscriber_id[0] if isinstance(subscriber_id, (list, tuple)) else subscriber_id)
            except ValueError:
                err_msg.append(f'Expected integer value for the subscriber_id parameter. Got [{subscriber_id}].')
            try:
                offset = int(offset[0] if isinstance(offset, (list, tuple)) else offset)
            except ValueError:
                err_msg.append(f'Expected integer value for the offset parameter. Got [{offset}].')
            if err_msg:
                logging.error('\n'.join(err_msg))
                send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                           {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': '<br>'.join(err_msg)})
                return
            with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
                with conn.cursor() as cur:
                    cur.execute("""select * from ack_message(%s, %s);""", (subscriber_id, offset))
                    res = cur.fetchone()
            self._set_response()
            self.wfile.write(json.dumps(res, default=str).encode('utf-8'))
        else:
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': "Missing parameter: topic"})

    # topic_reset route: /topic_reset?topic=<topic>&offset=<offset>
    def do_topic_reset(self):
        target_topic = self._query_params.get('topic')
        offset = self._query_params.get('offset', [1])
        if target_topic and offset:
            target_topic = target_topic[0]  if isinstance(target_topic, (list, tuple)) else target_topic
            try:
                offset = int(offset[0] if isinstance(offset, (list, tuple)) else offset)
            except ValueError:
                err_msg = f'Expected integer value for the subscriber_id parameter. Got [{offset}].'
                logging.error(err_msg)
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                               {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': err_msg})
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
                self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                                {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': err_msg})
                return
            
            logging.info(f'Set {res["topic_reset"]} subscribers of topic {target_topic} to reprocess any messages starting at offset {offset}')
            if self.command == 'GET':
                self._set_response()
                self.wfile.write(json.dumps({'topic': target_topic, 'offset': offset, 'subscriber_count': res['topic_reset']}, default=str).encode('utf-8'))
            else:
                self._set_response(headers={})
        else:
            logging.error("ERROR: /topic route missing parameters for topic")
            self.send_error(HTTPStatus.UNPROCESSABLE_ENTITY, 
                            {'http_status': HTTPStatus.UNPROCESSABLE_ENTITY, 'error_message': "Missing 'topic' parameter"})
        
        

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
    

