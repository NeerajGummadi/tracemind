import time
from datetime import datetime, timezone

import httpx
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from investigation_service.collectors.loki import LokiLogsCollector
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1

LOKI_CONFIG = """\
auth_enabled: false
server:
  http_listen_port: 3100
  grpc_listen_port: 9096
common:
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory
schema_config:
  configs:
    - from: 2024-01-01
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h
limits_config:
  allow_structured_metadata: false
"""


@pytest.fixture(scope="module")
def loki_container():
    """A genuinely real Loki server - logs are pushed to it over real HTTP,
    not mocked, per Milestone K's explicit requirement."""
    container = (
        DockerContainer("grafana/loki:3.5.3")
        .with_exposed_ports(3100)
        .with_copy_into_container(LOKI_CONFIG.encode("utf-8"), "/etc/loki/local-config.yaml")
        .with_command(["-config.file=/etc/loki/local-config.yaml"])
        .waiting_for(LogMessageWaitStrategy("Loki started").with_startup_timeout(30))
    )
    with container:
        yield container


@pytest.fixture
async def loki_base_url(loki_container):
    host = loki_container.get_container_host_ip()
    port = loki_container.get_exposed_port(3100)
    base_url = f"http://{host}:{port}"

    # The ingester needs a brief settle period after "ready" before it
    # reliably accepts writes (observed directly during manual verification
    # for this milestone - "Ingester not ready: waiting for 15s after being
    # ready" is a real, expected message from Loki itself).
    async with httpx.AsyncClient() as client:
        for _ in range(30):
            response = await client.get(f"{base_url}/ready")
            if response.status_code == 200:
                break
            time.sleep(1)

    return base_url


@pytest.mark.asyncio
async def test_collector_queries_a_real_loki_server_and_gets_real_grouped_evidence(loki_base_url):
    now_ns = time.time_ns()
    push_payload = {
        "streams": [
            {
                "stream": {"service": "payment-service", "environment": "prod"},
                "values": [
                    [str(now_ns), "Hikari connection acquisition timeout"],
                    [str(now_ns + 1), "Hikari connection acquisition timeout"],
                    [str(now_ns + 2), "Hikari connection acquisition timeout"],
                    [str(now_ns + 3), "Connection pool exhausted"],
                ],
            }
        ]
    }

    async with httpx.AsyncClient() as http_client:
        push_response = await http_client.post(f"{loki_base_url}/loki/api/v1/push", json=push_payload)
        assert push_response.status_code == 204

        now = datetime.now(timezone.utc)
        request = InvestigationRequestedV1(
            event_id="evt-1", schema_version="1.0", incident_id="INC-REAL-LOKI", primary_service="payment-service",
            environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
            trigger_signal_ids=["evt-1"], investigation_run_id="run-1", input_signal_version=1,
        )

        collector = LokiLogsCollector(client=http_client, base_url=loki_base_url, window_seconds=60, max_entries=200)

        # Loki ingestion isn't instantaneous - poll rather than assume it's queryable immediately.
        evidence = []
        for _ in range(20):
            evidence = await collector.collect(request)
            if evidence:
                break
            time.sleep(0.5)

    assert len(evidence) == 2
    timeout_evidence = next(e for e in evidence if "Hikari" in e.fact)
    assert timeout_evidence.occurrences == 3
    assert timeout_evidence.entity == "payment-service"
    pool_evidence = next(e for e in evidence if "pool exhausted" in e.fact)
    assert pool_evidence.occurrences == 1
