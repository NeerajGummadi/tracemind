package com.tracemind.incident.repository;

import com.tracemind.incident.domain.InvestigationRun;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface InvestigationRunRepository extends JpaRepository<InvestigationRun, UUID> {
}
