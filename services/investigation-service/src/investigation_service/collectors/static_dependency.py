import logging
from pathlib import Path

import yaml

from investigation_service.contracts.evidence import DependencyEvidence
from investigation_service.contracts.investigation_requested import InvestigationRequestedV1

logger = logging.getLogger(__name__)

Topology = dict[str, list[str]]


def load_topology(path: str) -> Topology:
    """Loads and validates the static dependency graph once, at startup.

    A missing file is a legitimate operational state (topology not
    configured yet) - it degrades to an empty topology, never crashes.
    A present-but-malformed file is a real config bug and is raised so
    main.py's startup fails fast, per Milestone L's explicit design choice:
    this loads once before traffic is served, so failing fast surfaces the
    bug immediately instead of silently returning empty dependency evidence
    for the service's entire uptime."""

    file_path = Path(path)
    if not file_path.exists():
        logger.warning("Dependency graph file not found at %s - dependency evidence will be empty", path)
        return {}

    with file_path.open() as f:
        raw = yaml.safe_load(f)

    return _validate(raw, path)


def _validate(raw: object, path: str) -> Topology:
    if not isinstance(raw, dict) or "services" not in raw:
        raise ValueError(f"Dependency graph {path} must be a mapping with a top-level 'services' key")

    services = raw["services"]
    if not isinstance(services, dict):
        raise ValueError(f"Dependency graph {path}: 'services' must be a mapping of service name -> definition")

    topology: Topology = {}
    for service_name, definition in services.items():
        if not isinstance(service_name, str) or not service_name:
            raise ValueError(f"Dependency graph {path}: service names must be non-empty strings, got {service_name!r}")

        definition = definition or {}
        if not isinstance(definition, dict):
            raise ValueError(f"Dependency graph {path}: definition for '{service_name}' must be a mapping")

        depends_on = definition.get("depends_on", [])
        if not isinstance(depends_on, list) or not all(isinstance(d, str) and d for d in depends_on):
            raise ValueError(f"Dependency graph {path}: 'depends_on' for '{service_name}' must be a list of non-empty strings")

        topology[service_name] = depends_on

    return topology


class StaticDependencyCollector:
    """Real DependencyCollector, backed by the static topology loaded once at
    startup (blueprint Section 13's MVP static-configuration source). Same
    never-crashes-the-investigation philosophy as Prometheus/Loki: an
    unknown primary service simply yields no evidence.

    Traversal is a bounded BFS from primaryService, guarded by a visited set
    so cycles terminate, capped at max_depth hops. A hop-2 edge is reported
    as the real (intermediate -> leaf) edge, never a fabricated direct edge
    from primaryService - the fact text explains the transitive path."""

    def __init__(self, topology: Topology, max_depth: int):
        self._topology = topology
        self._max_depth = max_depth

    async def collect(self, request: InvestigationRequestedV1) -> list[DependencyEvidence]:
        primary_service = request.primary_service
        if primary_service not in self._topology:
            return []

        evidence: list[DependencyEvidence] = []
        seen_edges: set[tuple[str, str]] = set()
        visited = {primary_service}
        # (service, hop) frontier, BFS order preserved for deterministic evidence IDs.
        frontier: list[tuple[str, int]] = [(primary_service, 0)]

        while frontier:
            service, hop = frontier.pop(0)
            if hop >= self._max_depth:
                continue

            for dep in self._topology.get(service, []):
                edge = (service, dep)
                if edge not in seen_edges:
                    seen_edges.add(edge)
                    evidence.append(
                        self._to_evidence(request, primary_service, service, dep, hop + 1, len(evidence) + 1)
                    )

                if dep not in visited:
                    visited.add(dep)
                    frontier.append((dep, hop + 1))

        return evidence

    def _to_evidence(
        self, request: InvestigationRequestedV1, primary_service: str, service: str, dep: str, hop: int, index: int
    ) -> DependencyEvidence:
        fact = (
            f"{service} depends on {dep}"
            if hop == 1
            else f"{service} depends on {dep} ({hop} hops from {primary_service})"
        )
        return DependencyEvidence(
            evidence_id=f"E-{request.incident_id}-DEP-{index}",
            entity=service,
            fact=fact,
            observed_at=request.last_observed_at,
            depends_on=dep,
        )
