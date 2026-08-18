package com.tracemind.connector.web;

import com.tracemind.connector.kafka.SignalPublishException;
import com.tracemind.connector.mapping.InvalidAlertPayloadException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Malformed/invalid payloads (client's fault, blueprint Section 27: never
 * retried) map to 400. Broker/publish failures (transient, retry-worthy)
 * map to 503.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException e) {
        String message = e.getBindingResult().getFieldErrors().stream()
                .findFirst()
                .map(err -> err.getField() + " " + err.getDefaultMessage())
                .orElse("Invalid request payload");
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.of(message));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.of("Malformed request body"));
    }

    @ExceptionHandler(InvalidAlertPayloadException.class)
    public ResponseEntity<ErrorResponse> handleInvalidAlert(InvalidAlertPayloadException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ErrorResponse.of(e.getMessage()));
    }

    @ExceptionHandler(SignalPublishException.class)
    public ResponseEntity<ErrorResponse> handlePublishFailure(SignalPublishException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(ErrorResponse.of("Unable to accept signal, try again"));
    }
}
