package com.tracemind.connector.kafka;

import com.tracemind.connector.contract.CanonicalSignalV1;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes to signals.received.v1, keyed by "environment:service" (blueprint
 * Section 7) so signals for the same service stay ordered before an incident
 * exists. Blocks on the send future so the caller never gets 202 unless
 * Kafka actually accepted the record (blueprint Section 34: don't
 * acknowledge success you can't guarantee) - bounded per Section 26 (no
 * infinite waits).
 */
@Component
public class CanonicalSignalPublisher {

    public static final String TOPIC = "signals.received.v1";

    private final KafkaTemplate<String, CanonicalSignalV1> kafkaTemplate;
    private final Duration sendTimeout;

    public CanonicalSignalPublisher(
            KafkaTemplate<String, CanonicalSignalV1> kafkaTemplate,
            @Value("${connector.kafka.send-timeout-ms:5000}") long sendTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.sendTimeout = Duration.ofMillis(sendTimeoutMs);
    }

    public void publish(CanonicalSignalV1 signal) {
        String key = signal.environment() + ":" + signal.service();
        try {
            kafkaTemplate.send(TOPIC, key, signal)
                    .get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (KafkaException e) {
            // KafkaTemplate.send() can fail synchronously (e.g. producer metadata
            // fetch timeout against an unreachable broker) rather than only via
            // the returned future.
            throw new SignalPublishException("Failed to publish signal " + signal.eventId(), e);
        } catch (ExecutionException e) {
            throw new SignalPublishException("Failed to publish signal " + signal.eventId(), e.getCause());
        } catch (TimeoutException e) {
            throw new SignalPublishException(
                    "Timed out publishing signal " + signal.eventId() + " after " + sendTimeout, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SignalPublishException("Interrupted while publishing signal " + signal.eventId(), e);
        }
    }
}
