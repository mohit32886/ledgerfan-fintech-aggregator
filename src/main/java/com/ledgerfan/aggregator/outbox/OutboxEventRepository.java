package com.ledgerfan.aggregator.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, UUID> {

    List<OutboxEventEntity> findByStatusOrderByCreatedAtAsc(OutboxEventStatus status, Pageable pageable);

    List<OutboxEventEntity> findTop50ByOrderByCreatedAtDesc();

    long countByStatus(OutboxEventStatus status);
}
