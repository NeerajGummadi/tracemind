package com.tracemind.connector.mapping;

import com.tracemind.connector.contract.CanonicalSignalV1;
import com.tracemind.connector.web.AlertmanagerAlert;
import com.tracemind.connector.web.AlertmanagerWebhookPayload;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PrometheusToCanonicalSignalMapperTest {

    private final PrometheusToCanonicalSignalMapper mapper = new PrometheusToCanonicalSignalMapper();

    @Test
    void mapsRequiredLabelsAndSplitsRemainingLabelsFromAttributes() {
        Instant startsAt = Instant.parse("2026-08-15T14:03:00Z");
        AlertmanagerAlert alert = new AlertmanagerAlert(
                "firing",
                Map.of(
                        "alertname", "DB_CONNECTION_PRESSURE",
                        "service", "payment-service",
                        "environment", "prod",
                        "severity", "CRITICAL",
                        "instance", "payment-service-2"),
                Map.of("summary", "Connection pool utilization reached 100%"),
                startsAt,
                null,
                "http://prometheus/graph",
                "abc123fingerprint");
        AlertmanagerWebhookPayload payload = new AlertmanagerWebhookPayload("firing", List.of(alert));

        List<CanonicalSignalV1> signals = mapper.map(payload);

        assertThat(signals).hasSize(1);
        CanonicalSignalV1 signal = signals.get(0);
        assertThat(signal.schemaVersion()).isEqualTo("1.0");
        assertThat(signal.source()).isEqualTo("PROMETHEUS");
        assertThat(signal.signalType()).isEqualTo("DB_CONNECTION_PRESSURE");
        assertThat(signal.service()).isEqualTo("payment-service");
        assertThat(signal.environment()).isEqualTo("prod");
        assertThat(signal.severity()).isEqualTo("CRITICAL");
        assertThat(signal.startedAt()).isEqualTo(startsAt);
        assertThat(signal.labels()).containsExactly(Map.entry("instance", "payment-service-2"));
        assertThat(signal.attributes()).containsExactly(Map.entry("summary", "Connection pool utilization reached 100%"));
        assertThat(signal.eventId()).startsWith("evt-");
    }

    @Test
    void throwsWhenRequiredLabelIsMissing() {
        AlertmanagerAlert alert = new AlertmanagerAlert(
                "firing",
                Map.of("alertname", "DB_CONNECTION_PRESSURE", "service", "payment-service"),
                Map.of(),
                Instant.now(),
                null,
                null,
                "abc123fingerprint");
        AlertmanagerWebhookPayload payload = new AlertmanagerWebhookPayload("firing", List.of(alert));

        assertThatThrownBy(() -> mapper.map(payload))
                .isInstanceOf(InvalidAlertPayloadException.class)
                .hasMessageContaining("environment");
    }

    @Test
    void eventIdIsDeterministicForSameFingerprintAndStartsAtButDiffersOnNewFiring() {
        Instant startsAt = Instant.parse("2026-08-15T14:03:00Z");
        AlertmanagerAlert firstDelivery = alertWith("fp-1", startsAt);
        AlertmanagerAlert retryDelivery = alertWith("fp-1", startsAt);
        AlertmanagerAlert newFiring = alertWith("fp-1", startsAt.plusSeconds(3600));

        String eventId1 = mapper.map(new AlertmanagerWebhookPayload("firing", List.of(firstDelivery))).get(0).eventId();
        String eventId2 = mapper.map(new AlertmanagerWebhookPayload("firing", List.of(retryDelivery))).get(0).eventId();
        String eventId3 = mapper.map(new AlertmanagerWebhookPayload("firing", List.of(newFiring))).get(0).eventId();

        assertThat(eventId1).isEqualTo(eventId2);
        assertThat(eventId1).isNotEqualTo(eventId3);
    }

    private AlertmanagerAlert alertWith(String fingerprint, Instant startsAt) {
        return new AlertmanagerAlert(
                "firing",
                Map.of("alertname", "X", "service", "svc", "environment", "prod", "severity", "CRITICAL"),
                Map.of(),
                startsAt,
                null,
                null,
                fingerprint);
    }
}
