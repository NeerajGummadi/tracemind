package com.tracemind.connector.web;

public record ErrorResponse(String status, String message) {

    public static ErrorResponse of(String message) {
        return new ErrorResponse("REJECTED", message);
    }
}
