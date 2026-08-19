package com.tracemind.incident.kafka;

import com.tracemind.incident.contract.CanonicalSignalV1;
import com.tracemind.incident.service.DuplicateSignalException;
import com.tracemind.incident.service.SignalIngestionService;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class SignalConsumerListenerTest {

    private final CanonicalSignalV1 signal = new CanonicalSignalV1(
            "evt-1", "1.0", "PROMETHEUS", "X", "svc", "prod", "CRITICAL",
            Instant.now(), Instant.now(), Map.of(), Map.of());

    @Test
    void acknowledgesOnSuccessfulIngestion() {
        SignalIngestionService service = mock(SignalIngestionService.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        SignalConsumerListener listener = new SignalConsumerListener(service);

        listener.onMessage(signal, ack);

        verify(ack).acknowledge();
    }

    @Test
    void acknowledgesOnDuplicateSignal() {
        SignalIngestionService service = mock(SignalIngestionService.class);
        doThrow(new DuplicateSignalException("evt-1", new RuntimeException()))
                .when(service).ingest(any());
        Acknowledgment ack = mock(Acknowledgment.class);
        SignalConsumerListener listener = new SignalConsumerListener(service);

        listener.onMessage(signal, ack);

        verify(ack).acknowledge();
    }

    @Test
    void doesNotAcknowledgeOnGenuineFailure() {
        SignalIngestionService service = mock(SignalIngestionService.class);
        doThrow(new RuntimeException("DB connection failure"))
                .when(service).ingest(any());
        Acknowledgment ack = mock(Acknowledgment.class);
        SignalConsumerListener listener = new SignalConsumerListener(service);

        assertThatThrownBy(() -> listener.onMessage(signal, ack))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("DB connection failure");

        verify(ack, never()).acknowledge();
    }
}
