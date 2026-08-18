package com.tracemind.connector.web;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Alertmanager always delivers a batch of alerts per webhook call (an
 * "alert group"), never a single alert - only the fields Connector Service
 * actually acts on are modeled here.
 */
public record AlertmanagerWebhookPayload(
        String status,
        @NotEmpty @Valid List<AlertmanagerAlert> alerts
) {
}
