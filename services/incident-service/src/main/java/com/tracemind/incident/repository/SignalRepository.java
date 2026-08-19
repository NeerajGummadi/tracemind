package com.tracemind.incident.repository;

import com.tracemind.incident.domain.Signal;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface SignalRepository extends JpaRepository<Signal, UUID> {
}
