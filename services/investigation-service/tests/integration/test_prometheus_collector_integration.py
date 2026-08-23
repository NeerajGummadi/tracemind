import asyncio
import socket
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from investigation_service.collectors.prometheus import PrometheusMetricsCollector
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1

DEMO_METRICS_TEXT = """\
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


class _DemoMetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = DEMO_METRICS_TEXT.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def demo_metrics_server_port():
    """A real local HTTP server serving the same fixed demo metrics as
    infrastructure/prometheus/demo_metrics_exporter.py - inlined here rather
    than imported, since that module lives outside the Python package."""
    port = _free_port()
    server = HTTPServer(("0.0.0.0", port), _DemoMetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield port
    server.shutdown()


@pytest.fixture(scope="module")
def prometheus_container(demo_metrics_server_port):
    """A genuinely real Prometheus server, scraping a genuinely real HTTP
    endpoint - not a mocked collector response, per Milestone J's explicit
    requirement not to fake the HTTP layer."""
    config = f"""\
global:
  scrape_interval: 2s
scrape_configs:
  - job_name: test-demo-metrics
    static_configs:
      - targets: ["host.docker.internal:{demo_metrics_server_port}"]
"""
    container = (
        DockerContainer("prom/prometheus:v3.14.0")
        .with_exposed_ports(9090)
        .with_copy_into_container(config.encode("utf-8"), "/etc/prometheus/prometheus.yml")
        .with_command(["--config.file=/etc/prometheus/prometheus.yml"])
        .waiting_for(LogMessageWaitStrategy("Server is ready to receive web requests").with_startup_timeout(30))
    )
    with container:
        yield container


@pytest.fixture
async def prometheus_base_url(prometheus_container):
    host = prometheus_container.get_container_host_ip()
    port = prometheus_container.get_exposed_port(9090)
    base_url = f"http://{host}:{port}"

    # Give Prometheus at least one real scrape cycle (2s interval configured
    # above) before querying, rather than racing it.
    async with httpx.AsyncClient() as client:
        for _ in range(30):
            response = await client.get(f"{base_url}/api/v1/targets")
            targets = response.json()["data"]["activeTargets"]
            if targets and targets[0]["health"] == "up":
                break
            await asyncio.sleep(1)
        else:
            pytest.fail("Prometheus never reported the demo metrics target as healthy")

    return base_url


@pytest.mark.asyncio
async def test_collector_queries_a_real_prometheus_server_and_gets_real_evidence(prometheus_base_url):
    now = datetime.now(timezone.utc)
    request = InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-REAL-PROM", primary_service="payment-service",
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"], investigation_run_id="run-1", input_signal_version=1,
    )

    async with httpx.AsyncClient() as http_client:
        collector = PrometheusMetricsCollector(
            client=http_client, base_url=prometheus_base_url, window_seconds=60, max_series=10
        )
        evidence = await collector.collect(request)

    assert len(evidence) == 3
    utilization = next(e for e in evidence if "utilization" in e.evidence_id)
    assert utilization.value == 100.0
    assert utilization.entity == "payment-service-db"
    assert "100.0%" in utilization.fact
