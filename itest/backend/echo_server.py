import gzip
import json
import os
import random
import re
import time
import urllib
import yaml

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

PORT = 9080

class EchoServer(BaseHTTPRequestHandler):
    """Simple server that should return a different response every time
    an endpoint is hit, including echoing back request information.

    For GET requests, return 'GET' followed by the timestamp and any incoming
        request headers.
    For POST requests, return 'POST', followed by the data, followed by
        the timestamp, followed by any incoming request headers.
    For HEAD requests, just send back a 200.
    """

    def _write_response(self, method, body=None, gzipped=False):
        self.send_header('Content-Type', 'application/json')
        self.send_header('Connection', 'close')
        self.end_headers()
        response = {
            'method': method,
            'timestamp': time.time(),
            'received_headers': self.headers.dict,
            'body': body,
            'null_value': None,
        }

        if gzipped:
            f = gzip.GzipFile(fileobj=self.wfile)
            f.write(json.dumps(response))
        else:
            self.wfile.write(json.dumps(response))

    def handle_bulk(self, cache_name, delimeter, namespace='backend.main'):
        """
        This function is targeted to test the behavior of receiving
        bulk endpoints. Here we introduce the feature of treating each id
        as a separate request. There are also valid ids (1-999) and invalid
        ids.
        We also handle special headers such as gzip encoding and returning text.
        We return text to verify that spectre doesn't crash when it doens't
        receive json and defaults to normal forwarding behavior.
        """
        yaml_file_path = os.getenv('SRV_CONFIGS_PATH') + '/' + namespace + '.yaml'
        yaml_file = open(yaml_file_path)
        cached_endpoints = yaml.load(yaml_file)['cached_endpoints']
        cache_name_configs = cached_endpoints[cache_name]
        if not cache_name_configs['bulk_support']:
            self.send_response(500)
            return 'ERROR in config file'
        self.send_response(200)

        if('test-content-type' in self.headers and self.headers['test-content-type'] == 'text'):
            self.send_header('Content-Type', 'text')
            self.end_headers()
            return 'this is text'

        if ('test-content-type' in self.headers and
                self.headers['test-content-type'] == 'application/json; charset=utf-8'
        ):
            self.send_header('Content-Type', 'application/json; charset=utf-8')
        else:
            self.send_header('Content-Type', 'application/json')
        self.end_headers()
        try:
            pattern = cache_name_configs['pattern']
            ids = re.match(pattern, self.path).group(2)
            ids = ids.split(delimeter)
            result = []
            for i in ids:
                try:
                    i = int(i)
                    if i > 0 and i < 1000:
                        result.append({'bulk_id': i, 'empty_array': [], 'null_value': None})
                except ValueError:
                    # If the ID is a string, then convert it back to unicode to emulate
                    # what would happen in a real webapp. This is, incidentally, what
                    # we want to test with TestGetBulkRequest::test_unicode_chars_in_bulk_response
                    result.append({'bulk_id': urllib.unquote(i), 'empty_array': [], 'null_value': None})
            if ('accept-encoding' in self.headers and self.headers['accept-encoding'] == 'gzip'):
                return 'hahaha giberish'
        except:
            # if there are any failures, just return an empty array
            result = []
        random.shuffle(result)
        return json.dumps(result)

    def do_GET(self):
        should_sleep = re.search('sleep=(\d+)', self.path)
        if should_sleep:
            delay_ms = int(should_sleep.group(1))
            time.sleep(delay_ms / 1000.0)

        should_error = re.search('error_status=([\d]+)', self.path)
        if should_error:
            self.send_response(int(should_error.group(1)))
            self.end_headers()
            return

        should_drop_connection = re.search('drop_connection=true', self.path)
        if should_drop_connection:
            # Don't send headers or anything, just close the connection
            # Similar to what happens on 502 errors
            return

        if self.path.startswith('/not_authorized'):
            self.send_response(403)
            self.end_headers()
            self.wfile.write('<html><boyd>403 Forbidden</body></html>')
            return
        elif self.path.endswith('/status'):
            self.send_response(200)
            self.end_headers()
            self.wfile.write('Backend is alive\n')
            return
        elif self.path.startswith('/bulk_requester_2'):
            result = self.handle_bulk('bulk_requester_default', ',')
            self.wfile.write(result)
            return
        elif self.path.startswith('/bulk_requester'):
            result = self.handle_bulk('bulk_requester_does_not_cache_missing_ids', '%2C')
            self.wfile.write(result)
            return
        # Discover custom defined test bulk endpoints
        elif 'X-Casper-Bulk' in self.headers:
            result = self.handle_bulk(self.headers['X-Casper-Bulk'], ',', 'custom.main')
            self.wfile.write(result)
            return
        else:
            self.send_response(200)

        # Send some response headers
        self.send_header('Some-Header', 'abc')
        self.send_header('Uncacheable-Header', 'def')

        self._write_response('GET', gzipped=self.path.endswith('/gzipped'))

    def do_POST(self):
        self.send_response(200)
        length = int(self.headers.getheader('content-length'))
        body = self.rfile.read(length)
        self._write_response('POST', body=body)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()


def main():
    server = HTTPServer(('', PORT), EchoServer)
    server.serve_forever()


if __name__ == '__main__':
    main()
