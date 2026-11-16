package com.ledgerfan.aggregator.outbox;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

/**
 * Command payload for posting a ledger transaction using the zero-dual-write outbox pattern.
 */
public record LedgerTransferRequest(
    @NotBlank String accountId,
    @NotBlank String counterpartyAccountId,
    @NotNull @DecimalMin("0.01") BigDecimal amount,
    @NotBlank String currency,
    @NotBlank String transactionType,
    String reference
) {
}
