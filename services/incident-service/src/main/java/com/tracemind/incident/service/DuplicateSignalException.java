package com.tracemind.incident.service;

/** The event_id already exists - this signal was fully processed by an earlier delivery. Safe to ack and skip. */
public class DuplicateSignalException extends RuntimeException {

    private final String eventId;

    public DuplicateSignalException(String eventId, Throwable cause) {
        super("Signal " + eventId + " already processed", cause);
        this.eventId = eventId;
    }

    public String getEventId() {
        return eventId;
    }
}
