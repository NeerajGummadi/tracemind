package com.tracemind.connector.kafka;

/** Kafka did not confirm the publish within the bounded wait - a transient, retry-worthy failure. */
public class SignalPublishException extends RuntimeException {

    public SignalPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
