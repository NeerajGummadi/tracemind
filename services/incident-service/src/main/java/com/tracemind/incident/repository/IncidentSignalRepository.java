package com.tracemind.incident.repository;

import com.tracemind.incident.domain.IncidentSignal;
import com.tracemind.incident.domain.IncidentSignalId;
import org.springframework.data.jpa.repository.JpaRepository;

public interface IncidentSignalRepository extends JpaRepository<IncidentSignal, IncidentSignalId> {
}
