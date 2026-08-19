package com.tracemind.incident.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.domain.OutboxEvent;
import com.tracemind.incident.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Reads unpublished outbox_events and delivers them to Kafka (blueprint
 * Section 10). Runs inside Incident Service - a separate poller component,
 * not a separate service (invariant 15).
 */
@Component
public class OutboxPublisher {

    public static final String TOPIC = "investigation.requested.v1";

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> outboxKafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int batchSize;
    private final Duration sendTimeout;

    public OutboxPublisher(
            OutboxEventRepository outboxEventRepository,
            KafkaTemplate<String, String> outboxKafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${outbox.publisher.batch-size:50}") int batchSize,
            @Value("${outbox.publisher.send-timeout-ms:5000}") long sendTimeoutMs) {
        this.outboxEventRepository = outboxEventRepository;
        this.outboxKafkaTemplate = outboxKafkaTemplate;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
        this.sendTimeout = Duration.ofMillis(sendTimeoutMs);
    }

    /**
     * On a publish failure, this stops the batch early rather than retrying
     * the remaining slots immediately - the gap until the next scheduled
     * poll is the retry backoff, avoiding a tight in-process retry loop.
     */
    @Scheduled(fixedDelayString = "${outbox.publisher.poll-interval-ms:1000}")
    public void poll() {
        for (int i = 0; i < batchSize; i++) {
            try {
                if (!claimAndPublishOne()) {
                    return; // nothing pending right now
                }
            } catch (Exception e) {
                log.warn("Outbox publish attempt failed, leaving row(s) pending for the next poll cycle", e);
                return;
            }
        }
    }

    /**
     * One row, one transaction: claim (with row lock), publish, mark
     * published, commit. Any exception rolls the whole thing back, so a
     * failed publish leaves the row exactly as claimOnePending() found it -
     * still PENDING, published_at still null.
     */
    @Transactional
    public boolean claimAndPublishOne() {
        Optional<OutboxEvent> claimed = outboxEventRepository.claimOnePending();
        if (claimed.isEmpty()) {
            return false;
        }
        OutboxEvent event = claimed.get();

        String key = extractIncidentId(event.getPayload());
        try {
            outboxKafkaTemplate.send(TOPIC, key, event.getPayload())
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (KafkaException e) {
            throw new OutboxPublishException("Failed to publish outbox event " + event.getId(), e);
        } catch (ExecutionException e) {
            throw new OutboxPublishException("Failed to publish outbox event " + event.getId(), e.getCause());
        } catch (TimeoutException e) {
            throw new OutboxPublishException(
                    "Timed out publishing outbox event " + event.getId() + " after " + sendTimeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OutboxPublishException("Interrupted while publishing outbox event " + event.getId(), e);
        }

        event.markPublished(Instant.now());
        outboxEventRepository.save(event);
        return true;
    }

    private String extractIncidentId(String payloadJson) {
        try {
            JsonNode node = objectMapper.readTree(payloadJson);
            return node.get("incidentId").asText();
        } catch (Exception e) {
            throw new IllegalStateException("Outbox payload is not valid JSON or missing incidentId", e);
        }
    }
}
