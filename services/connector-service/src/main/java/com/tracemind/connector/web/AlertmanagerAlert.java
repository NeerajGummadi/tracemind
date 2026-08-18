package com.tracemind.connector.web;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.Map;

/**
 * One entry in an Alertmanager webhook's "alerts" array.
 * See https://prometheus.io/docs/alerting/latest/configuration/#webhook_config
 */
public record AlertmanagerAlert(
        String status,
        @NotEmpty Map<String, String> labels,
        Map<String, String> annotations,
        @NotNull Instant startsAt,
        Instant endsAt,
        String generatorURL,
        @NotBlank String fingerprint
) {
}
