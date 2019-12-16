#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import logging
from urllib.parse import urlparse, parse_qs
import os
from psycopg2 import connect as db_connect
from psycopg2.extras import RealDictCursor

from IPython import embed

DEFAULT_PORT=8888

DB_URI = os.environ.get('DB_URI', 'postgresql://queues@db:5432/queues')


class QRequestHandler(BaseHTTPRequestHandler):
    def _set_response(self, headers={'Content-type': 'text/html'}):
        self.send_response(200)
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
    
    def do_topics():
        res = None
        with db_connect(DB_URI, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                cur.execute("""select * from topics;""")
                res = cur.fetchall()
        
        if res is not None:
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
    

