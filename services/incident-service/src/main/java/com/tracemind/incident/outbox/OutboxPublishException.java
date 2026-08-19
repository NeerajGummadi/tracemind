package com.tracemind.incident.outbox;

/** Kafka did not confirm the publish within the bounded wait - a transient, retry-worthy failure. */
public class OutboxPublishException extends RuntimeException {

    public OutboxPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
