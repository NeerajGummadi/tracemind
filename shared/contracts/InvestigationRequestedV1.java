package com.tracemind.contracts;

import java.time.Instant;
import java.util.List;

/**
 * Canonical source of truth for the investigation.requested.v1 event contract
 * (see docs/architecture/engineering-blueprint.md, Section 6).
 *
 * Reference definition only, not a compiled build artifact — see the note in
 * CanonicalSignalV1.java in this same directory.
 */
public record InvestigationRequestedV1(
        String eventId,
        String schemaVersion,
        String incidentId,
        String primaryService,
        String environment,
        String severity,
        Instant firstObservedAt,
        Instant lastObservedAt,
        List<String> triggerSignalIds
) {
}
