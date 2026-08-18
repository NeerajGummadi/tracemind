package com.tracemind.connector.web;

import java.util.List;

public record IngestResponse(String status, List<String> eventIds) {

    public static IngestResponse accepted(List<String> eventIds) {
        return new IngestResponse("ACCEPTED", eventIds);
    }
}
