package com.tracemind.connector.web;

import com.tracemind.connector.contract.CanonicalSignalV1;
import com.tracemind.connector.kafka.CanonicalSignalPublisher;
import com.tracemind.connector.mapping.PrometheusToCanonicalSignalMapper;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class AlertIngestController {

    private final PrometheusToCanonicalSignalMapper mapper;
    private final CanonicalSignalPublisher publisher;

    public AlertIngestController(PrometheusToCanonicalSignalMapper mapper, CanonicalSignalPublisher publisher) {
        this.mapper = mapper;
        this.publisher = publisher;
    }

    @PostMapping("/integrations/prometheus/alerts")
    public ResponseEntity<IngestResponse> ingest(@Valid @RequestBody AlertmanagerWebhookPayload payload) {
        List<CanonicalSignalV1> signals = mapper.map(payload);

        for (CanonicalSignalV1 signal : signals) {
            publisher.publish(signal);
        }

        List<String> eventIds = signals.stream().map(CanonicalSignalV1::eventId).toList();
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(IngestResponse.accepted(eventIds));
    }
}
