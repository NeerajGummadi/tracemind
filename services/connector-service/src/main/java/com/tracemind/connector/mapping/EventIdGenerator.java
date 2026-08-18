package com.tracemind.connector.mapping;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Derives a deterministic eventId from Alertmanager's own alert fingerprint
 * and startsAt, so that repeated webhook deliveries of the same firing
 * produce the same eventId (absorbed by Incident Service's Postgres
 * event_id UNIQUE constraint), while a new firing of a previously-resolved
 * alert - a different startsAt - produces a new one.
 */
public final class EventIdGenerator {

    private EventIdGenerator() {
    }

    public static String generate(String fingerprint, Instant startsAt) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(
                    (fingerprint + "|" + startsAt).getBytes(StandardCharsets.UTF_8));
            String hex = HexFormat.of().formatHex(hash, 0, 8);
            return "evt-" + hex;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required and must always be available", e);
        }
    }
}
