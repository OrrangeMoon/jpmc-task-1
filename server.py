# market_simulation.py
import csv
from datetime import timedelta, datetime
from random import normalvariate, random
import dateutil.parser
from threading import Thread
import http.server
import json
import os
from socketserver import ThreadingMixIn

# Configuration
REALTIME = True
SIM_LENGTH = timedelta(days=365 * 5)
MARKET_OPEN = datetime.today().replace(hour=0, minute=30, second=0)
SPD = (2.0, 6.0, 0.1)
PX = (60.0, 150.0, 1)
FREQ = (12, 36, 50)
OVERLAP = 4

def bwalk(min_val, max_val, std_dev):
    """Generates a bounded random walk for market simulation."""
    range_val = max_val - min_val
    while True:
        max_val += normalvariate(0, std_dev)
        yield abs((max_val % (range_val * 2)) - range_val) + min_val

def market(t0=MARKET_OPEN):
    """Generates a series of market conditions."""
    for hours, px, spd in zip(bwalk(*FREQ), bwalk(*PX), bwalk(*SPD)):
        yield t0, px, spd
        t0 += timedelta(hours=abs(hours))

def orders(hist):
    """Generates orders based on market conditions."""
    for t, px, spd in hist:
        stock = 'ABC' if random() > 0.5 else 'DEF'
        side = 'sell' if random() > 0.5 else 'buy'
        order = round(normalvariate(px + (spd / 2 if side == 'sell' else -2), spd / OVERLAP), 2)
        size = int(abs(normalvariate(0, 100)))
        yield t, stock, side, order, size

def generate_csv():
    """Writes generated orders to a CSV file."""
    with open('test.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        for item in orders(market()):
            if item[0] > MARKET_OPEN + SIM_LENGTH:
                break
            writer.writerow(item)

def read_csv():
    """Reads order data from CSV file."""
    with open('test.csv', 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            yield dateutil.parser.parse(row[0]), row[1], row[2], float(row[3]), int(row[4])

# server.py
class ThreadedHTTPServer(ThreadingMixIn, http.server.HTTPServer):
    """HTTP Server with threading and proper shutdown handling."""
    allow_reuse_address = True

class ServerHandler(http.server.SimpleHTTPRequestHandler):
    """Handles HTTP requests by routing them to registered paths."""
    routes = {}

    def do_GET(self):
        self.handle_request()

    def handle_request(self):
        path = self.path.split('?')[0]
        if path in self.routes:
            self.routes[path](self)
        else:
            self.send_error(404, "Path not found.")

    @staticmethod
    def route(path):
        def decorator(func):
            ServerHandler.routes[path] = func
            return func
        return decorator

    def send_json(self, content):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(content).encode('utf-8'))

@ServerHandler.route("/market")
def market_status(handler):
    data = {'message': 'Market is active.'}  # Example response
    handler.send_json(data)

def run_server():
    server_address = ('', 8080)
    httpd = ThreadedHTTPServer(server_address, ServerHandler)
    print("Server started on port 8080.")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()

# app.py
if __name__ == '__main__':
    if not os.path.exists('test.csv'):
        print("Generating new market data...")
        generate_csv()
    run_server()
