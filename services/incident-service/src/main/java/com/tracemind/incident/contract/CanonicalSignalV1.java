package com.tracemind.incident.contract;

import java.time.Instant;
import java.util.Map;

/**
 * Incident Service's copy of the canonical signal contract. Canonical
 * definition lives at shared/contracts/CanonicalSignalV1.java - keep this in
 * sync by hand (see the note there for why there's no shared module yet).
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
