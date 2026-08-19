package com.tracemind.incident.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tracemind.incident.contract.CanonicalSignalV1;
import com.tracemind.incident.domain.Signal;
import com.tracemind.incident.repository.SignalRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SignalIngestionService {

    private final SignalRepository signalRepository;
    private final IncidentCorrelationService correlationService;
    private final ObjectMapper objectMapper;

    public SignalIngestionService(
            SignalRepository signalRepository,
            IncidentCorrelationService correlationService,
            ObjectMapper objectMapper) {
        this.signalRepository = signalRepository;
        this.correlationService = correlationService;
        this.objectMapper = objectMapper;
    }

    /**
     * Signal insert + correlation + incident write + outbox write all happen
     * in this one transaction (blueprint invariant 5). The signal insert is
     * flushed immediately and is the first statement, so any constraint
     * violation from it can only be the event_id UNIQUE constraint - that's
     * how DuplicateSignalException stays unambiguous without inspecting the
     * exception message.
     */
    @Transactional
    public void ingest(CanonicalSignalV1 canonicalSignal) {
        Signal signal = Signal.from(canonicalSignal, objectMapper);
        try {
            signalRepository.saveAndFlush(signal);
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateSignalException(canonicalSignal.eventId(), e);
        }

        correlationService.correlateAndPersist(signal, canonicalSignal);
    }
}
