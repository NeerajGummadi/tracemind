package com.tracemind.connector.mapping;

/** Semantic validation failure that Bean Validation can't express (e.g. a missing required label). */
public class InvalidAlertPayloadException extends RuntimeException {

    public InvalidAlertPayloadException(String message) {
        super(message);
    }
}
