package com.tracemind.contracts;

import java.time.Instant;
import java.util.Map;

/**
 * Canonical source of truth for TraceMind's vendor-neutral signal contract
 * (see docs/architecture/engineering-blueprint.md, Section 6).
 *
 * This file is a reference definition, not a compiled build artifact. Each
 * service that needs this type currently keeps its own copy under its own
 * package (e.g. com.tracemind.connector.contract, com.tracemind.incident.contract)
 * and must be kept in sync with this definition by hand. This duplication is
 * an intentional Phase 1 simplification — extract a shared module only when
 * a concrete need (a third consumer, drift between copies) appears.
 */
public record CanonicalSignalV1(
        String eventId,
        String schemaVersion,
        String source,
        String signalType,
        String service,
        String environment,
        String severity,
        Instant startedAt,
        Instant observedAt,
        Map<String, String> labels,
        Map<String, Object> attributes
) {
}
