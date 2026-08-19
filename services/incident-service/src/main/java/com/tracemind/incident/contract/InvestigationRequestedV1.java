package com.tracemind.incident.contract;

import java.time.Instant;
import java.util.List;

/**
 * Incident Service's copy of the investigation.requested.v1 contract.
 * Canonical definition lives at shared/contracts/InvestigationRequestedV1.java.
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
