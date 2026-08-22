from datetime import datetime, timezone

import pytest

from investigation_service.collectors.static_dependency import StaticDependencyCollector, load_topology
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1


def make_request(primary_service: str = "payment-service") -> InvestigationRequestedV1:
    now = datetime(2026, 8, 22, 12, 0, 0, tzinfo=timezone.utc)
    return InvestigationRequestedV1(
        event_id="evt-1", schema_version="1.0", incident_id="INC-1", primary_service=primary_service,
        environment="prod", severity="CRITICAL", first_observed_at=now, last_observed_at=now,
        trigger_signal_ids=["evt-1"],
    )


def write_topology(tmp_path, content: str) -> str:
    path = tmp_path / "service-dependencies.yml"
    path.write_text(content)
    return str(path)


@pytest.mark.asyncio
async def test_direct_dependency_lookup():
    topology = {"payment-service": ["payment-service-db"]}
    collector = StaticDependencyCollector(topology=topology, max_depth=1)

    evidence = await collector.collect(make_request())

    assert len(evidence) == 1
    assert evidence[0].entity == "payment-service"
    assert evidence[0].depends_on == "payment-service-db"
    assert evidence[0].fact == "payment-service depends on payment-service-db"


@pytest.mark.asyncio
async def test_multiple_direct_dependencies_all_returned():
    topology = {"payment-service": ["payment-service-db", "redis", "kafka"]}
    collector = StaticDependencyCollector(topology=topology, max_depth=1)

    evidence = await collector.collect(make_request())

    assert {e.depends_on for e in evidence} == {"payment-service-db", "redis", "kafka"}


@pytest.mark.asyncio
async def test_bounded_second_hop_traversal():
    topology = {
        "payment-service": ["fraud-service"],
        "fraud-service": ["fraud-service-db"],
    }
    collector = StaticDependencyCollector(topology=topology, max_depth=2)

    evidence = await collector.collect(make_request())

    assert len(evidence) == 2
    direct = next(e for e in evidence if e.depends_on == "fraud-service")
    assert direct.entity == "payment-service"
    assert direct.fact == "payment-service depends on fraud-service"

    second_hop = next(e for e in evidence if e.depends_on == "fraud-service-db")
    assert second_hop.entity == "fraud-service"
    assert "2 hops from payment-service" in second_hop.fact


@pytest.mark.asyncio
async def test_max_depth_one_excludes_second_hop():
    topology = {
        "payment-service": ["fraud-service"],
        "fraud-service": ["fraud-service-db"],
    }
    collector = StaticDependencyCollector(topology=topology, max_depth=1)

    evidence = await collector.collect(make_request())

    assert len(evidence) == 1
    assert evidence[0].depends_on == "fraud-service"


@pytest.mark.asyncio
async def test_cycle_does_not_hang_or_duplicate():
    topology = {
        "payment-service": ["fraud-service"],
        "fraud-service": ["payment-service"],
    }
    collector = StaticDependencyCollector(topology=topology, max_depth=5)

    evidence = await collector.collect(make_request())

    assert len(evidence) == 2
    depends_on = {e.depends_on for e in evidence}
    assert depends_on == {"fraud-service", "payment-service"}


@pytest.mark.asyncio
async def test_diamond_dependency_produces_no_duplicate_evidence():
    topology = {
        "payment-service": ["service-a", "service-b"],
        "service-a": ["shared-db"],
        "service-b": ["shared-db"],
    }
    collector = StaticDependencyCollector(topology=topology, max_depth=2)

    evidence = await collector.collect(make_request())

    edges = [(e.entity, e.depends_on) for e in evidence]
    assert len(edges) == len(set(edges))
    assert ("service-a", "shared-db") in edges
    assert ("service-b", "shared-db") in edges


@pytest.mark.asyncio
async def test_unknown_service_returns_empty_evidence():
    collector = StaticDependencyCollector(topology={"payment-service": ["payment-service-db"]}, max_depth=2)

    evidence = await collector.collect(make_request(primary_service="unknown-service"))

    assert evidence == []


@pytest.mark.asyncio
async def test_service_with_no_dependencies_returns_empty_evidence():
    collector = StaticDependencyCollector(topology={"notification-service": []}, max_depth=2)

    evidence = await collector.collect(make_request(primary_service="notification-service"))

    assert evidence == []


@pytest.mark.asyncio
async def test_evidence_ids_are_deterministic_across_calls():
    topology = {"payment-service": ["payment-service-db", "redis"]}
    collector = StaticDependencyCollector(topology=topology, max_depth=1)

    first = await collector.collect(make_request())
    second = await collector.collect(make_request())

    assert [e.evidence_id for e in first] == [e.evidence_id for e in second]
    assert first[0].evidence_id == "E-INC-1-DEP-1"
    assert first[1].evidence_id == "E-INC-1-DEP-2"


def test_load_topology_missing_file_returns_empty_dict(tmp_path):
    missing_path = str(tmp_path / "does-not-exist.yml")

    topology = load_topology(missing_path)

    assert topology == {}


def test_load_topology_parses_valid_file(tmp_path):
    path = write_topology(
        tmp_path,
        """
        services:
          payment-service:
            depends_on:
              - payment-service-db
              - redis
          notification-service:
            depends_on:
              - kafka
        """,
    )

    topology = load_topology(path)

    assert topology == {
        "payment-service": ["payment-service-db", "redis"],
        "notification-service": ["kafka"],
    }


def test_load_topology_service_with_no_depends_on_key(tmp_path):
    path = write_topology(tmp_path, "services:\n  standalone-service: {}\n")

    topology = load_topology(path)

    assert topology == {"standalone-service": []}


@pytest.mark.parametrize(
    "content",
    [
        "not-a-mapping-at-all",
        "services: not-a-mapping",
        "services:\n  payment-service:\n    depends_on: not-a-list\n",
        "services:\n  payment-service:\n    depends_on:\n      - \"\"\n",
        "services:\n  payment-service:\n    depends_on:\n      - 42\n",
    ],
)
def test_load_topology_malformed_file_raises(tmp_path, content):
    path = write_topology(tmp_path, content)

    with pytest.raises(ValueError):
        load_topology(path)
