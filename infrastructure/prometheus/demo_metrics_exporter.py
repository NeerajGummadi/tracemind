"""Smallest possible deterministic metrics source for local Prometheus
scraping - not a TraceMind service, just a local demo fixture so Prometheus
has something real to scrape for the DB-connection-pool-exhaustion scenario
used throughout Milestones G-I. Stdlib only, no new dependency.

Run directly: python3 demo_metrics_exporter.py [port, default 9105]
"""

import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

METRICS_TEXT = """\
# HELP db_connection_pool_active Active DB connections in the pool
# TYPE db_connection_pool_active gauge
db_connection_pool_active{service="payment-service",environment="prod"} 100

# HELP db_connection_pool_max Maximum DB connection pool size
# TYPE db_connection_pool_max gauge
db_connection_pool_max{service="payment-service",environment="prod"} 100

# HELP db_connection_pool_utilization_percent DB connection pool utilization percentage
# TYPE db_connection_pool_utilization_percent gauge
db_connection_pool_utilization_percent{service="payment-service",environment="prod"} 100
"""


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = METRICS_TEXT.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # keep local demo output quiet


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9105
    server = HTTPServer(("0.0.0.0", port), MetricsHandler)
    print(f"Demo metrics exporter serving /metrics on :{port}")
    server.serve_forever()
