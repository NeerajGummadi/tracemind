package com.tracemind.incident.kafka;

import com.tracemind.incident.contract.CanonicalSignalV1;
import com.tracemind.incident.service.DuplicateSignalException;
import com.tracemind.incident.service.SignalIngestionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class SignalConsumerListener {

    public static final String TOPIC = "signals.received.v1";

    private static final Logger log = LoggerFactory.getLogger(SignalConsumerListener.class);

    private final SignalIngestionService signalIngestionService;

    public SignalConsumerListener(SignalIngestionService signalIngestionService) {
        this.signalIngestionService = signalIngestionService;
    }

    /**
     * Any exception other than DuplicateSignalException propagates out of
     * this method uncaught, so ack.acknowledge() is never reached and the
     * offset is not committed - Kafka redelivers.
     */
    @KafkaListener(topics = TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(CanonicalSignalV1 signal, Acknowledgment ack) {
        try {
            signalIngestionService.ingest(signal);
        } catch (DuplicateSignalException e) {
            log.info("Skipping already-processed signal {}", e.getEventId());
        }
        ack.acknowledge();
    }
}
