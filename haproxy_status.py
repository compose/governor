#!/usr/bin/env python

from BaseHTTPServer import BaseHTTPRequestHandler
import sys, yaml, socket
import requests

f = open(sys.argv[1], "r")
config = yaml.load(f.read())
f.close()

class StatusHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        return self.do_ANY()
    def do_OPTIONS(self):
        return self.do_ANY()
    def do_ANY(self):
        base_url = "http://localhost:" + str(config["api_port"])
        is_leader_resp = requests.get(base_url + "/ha/is_leader")
        running_as_leader_resp = requests.get(
            base_url + "/service/running_as_leader") 
        is_healthy_resp = requests.get(base_url + "/service/is_healthy")
        if is_healthy_resp.json()["data"]["is_healthy"] and \
            is_leader_resp.json()["data"]["is_leader"] and \
            running_as_leader_resp.json()["data"]["running_as_leader"]:
          self.send_response(200)
        else:
          self.send_response(503)
        self.end_headers()
        self.wfile.write('\r\n')
        return

try:
    from BaseHTTPServer import HTTPServer
    host, port = config["haproxy_status"]["listen"].split(":")
    server = HTTPServer((host, int(port)), StatusHandler)
    print 'listening on %s:%s' % (host, port)
    server.serve_forever()
except KeyboardInterrupt:
    print('^C received, shutting down server')
    server.socket.close()
