package com.tracemind.incident.domain;

import java.util.List;

/**
 * Not specified by the blueprint - severity escalation needs some ordering,
 * so this is a Phase 1 default, not a blueprint requirement. Unrecognized
 * values rank lowest, so they never trigger escalation but can always be
 * escalated over.
 */
final class SeverityRanking {

    private static final List<String> ORDER = List.of("INFO", "WARNING", "HIGH", "CRITICAL");

    private SeverityRanking() {
    }

    static boolean isHigherThan(String candidate, String current) {
        return rank(candidate) > rank(current);
    }

    private static int rank(String severity) {
        int index = ORDER.indexOf(severity == null ? null : severity.toUpperCase());
        return index; // -1 for unrecognized, correctly ranks lowest
    }
}
