package com.tracemind.incident.kafka;

import com.tracemind.incident.service.InvestigationResultService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class InvestigationResultConsumerListener {

    public static final String TOPIC = "investigation.results.v1";

    private final InvestigationResultService investigationResultService;

    public InvestigationResultConsumerListener(InvestigationResultService investigationResultService) {
        this.investigationResultService = investigationResultService;
    }

    /**
     * Any exception propagates out uncaught, so ack.acknowledge() is never
     * reached and Kafka redelivers - same pattern as SignalConsumerListener.
     * Ordinary duplicate/out-of-order deliveries are handled deterministically
     * inside handleResult() (idempotent per-run status guards), not here.
     */
    @KafkaListener(topics = TOPIC, groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "investigationResultKafkaListenerContainerFactory")
    public void onMessage(String rawJson, Acknowledgment ack) {
        investigationResultService.handleResult(rawJson);
        ack.acknowledge();
    }
}
