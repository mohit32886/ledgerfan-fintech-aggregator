package com.ledgerfan.aggregator.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface LedgerTransactionRepository extends JpaRepository<LedgerTransactionEntity, UUID> {

    List<LedgerTransactionEntity> findTop20ByAccountIdOrderByCreatedAtDesc(String accountId);
}
