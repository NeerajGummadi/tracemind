package com.tracemind.connector.mapping;

import com.tracemind.connector.contract.CanonicalSignalV1;
import com.tracemind.connector.web.AlertmanagerAlert;
import com.tracemind.connector.web.AlertmanagerWebhookPayload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Normalizes Alertmanager's webhook payload into CanonicalSignalV1 (blueprint
 * Section 6) - the only place in the platform allowed to understand
 * Alertmanager's wire format (blueprint invariant 6).
 */
@Component
public class PrometheusToCanonicalSignalMapper {

    private static final String LABEL_ALERTNAME = "alertname";
    private static final String LABEL_SERVICE = "service";
    private static final String LABEL_ENVIRONMENT = "environment";
    private static final String LABEL_SEVERITY = "severity";

    private static final Set<String> PROMOTED_LABELS =
            Set.of(LABEL_ALERTNAME, LABEL_SERVICE, LABEL_ENVIRONMENT, LABEL_SEVERITY);

    public List<CanonicalSignalV1> map(AlertmanagerWebhookPayload payload) {
        return payload.alerts().stream().map(this::mapAlert).toList();
    }

    private CanonicalSignalV1 mapAlert(AlertmanagerAlert alert) {
        Map<String, String> labels = alert.labels();

        String signalType = requireLabel(labels, LABEL_ALERTNAME);
        String service = requireLabel(labels, LABEL_SERVICE);
        String environment = requireLabel(labels, LABEL_ENVIRONMENT);
        String severity = requireLabel(labels, LABEL_SEVERITY);

        Map<String, String> remainingLabels = new HashMap<>(labels);
        remainingLabels.keySet().removeAll(PROMOTED_LABELS);

        Map<String, Object> attributes = alert.annotations() == null
                ? Map.of()
                : Map.copyOf(alert.annotations());

        String eventId = EventIdGenerator.generate(alert.fingerprint(), alert.startsAt());

        return new CanonicalSignalV1(
                eventId,
                "1.0",
                "PROMETHEUS",
                signalType,
                service,
                environment,
                severity,
                alert.startsAt(),
                Instant.now(),
                Map.copyOf(remainingLabels),
                attributes
        );
    }

    private String requireLabel(Map<String, String> labels, String key) {
        String value = labels.get(key);
        if (value == null || value.isBlank()) {
            throw new InvalidAlertPayloadException("Alert is missing required label '" + key + "'");
        }
        return value;
    }
}
