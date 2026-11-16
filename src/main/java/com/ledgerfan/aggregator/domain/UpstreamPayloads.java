package com.ledgerfan.aggregator.domain;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * Domain payload contracts returned by the 12 federated banking upstreams (REST + GraphQL).
 */
public final class UpstreamPayloads {

    private UpstreamPayloads() {
    }

    public record CheckingAccountSummary(
        String accountId,
        String iban,
        String currency,
        BigDecimal availableBalance,
        BigDecimal currentBalance,
        String accountStatus
    ) {
    }

    public record KycComplianceProfile(
        String kycTier,
        int amlRiskScore,
        boolean pepStatus,
        boolean sanctionsCleared,
        Instant lastVerifiedAt
    ) {
    }

    public record GeneralLedgerPosition(
        String ledgerAccountId,
        BigDecimal postedBalance,
        BigDecimal pendingDebits,
        BigDecimal pendingCredits,
        BigDecimal netIntradayPosition,
        long lastEntrySequence
    ) {
    }

    public record CreditCardPortfolio(
        int activeCardCount,
        BigDecimal totalCreditLimit,
        BigDecimal currentBalance,
        double utilizationRatio,
        LocalDate nextStatementDate
    ) {
    }

    public record DebitCardControls(
        boolean cardFrozen,
        boolean contactlessEnabled,
        boolean internationalSpendEnabled,
        BigDecimal dailyAtmLimit,
        BigDecimal posLimit
    ) {
    }

    public record FraudRiskSignals(
        String riskBand,
        double velocityScore,
        double deviceTrustScore,
        int activeAlertCount,
        boolean stepUpAuthRequired
    ) {
    }

    public record LoanMortgageSummary(
        int activeLoanCount,
        BigDecimal outstandingPrincipal,
        BigDecimal nextInstallmentAmount,
        LocalDate nextPaymentDueDate,
        BigDecimal weightedAverageApr
    ) {
    }

    public record InvestmentWealthSummary(
        BigDecimal portfolioMarketValue,
        BigDecimal dayChangeAmount,
        double unrealizedGainPct,
        Map<String, BigDecimal> assetAllocation
    ) {
    }

    public record RewardsLoyaltySummary(
        String tierName,
        long pointsBalance,
        BigDecimal cashbackAccruedYtd,
        List<String> activeMerchantBoosts
    ) {
    }

    public record StandingOrdersSummary(
        int activeStandingOrders,
        int activeDirectDebits,
        BigDecimal next7DaysOutflowTotal,
        LocalDate nextScheduledDate
    ) {
    }

    public record FxTreasuryRates(
        String baseCurrency,
        Map<String, BigDecimal> rates,
        Instant quoteLockExpiresAt
    ) {
    }

    public record CustomerPreferences(
        String preferredLocale,
        String taxResidencyCountry,
        boolean paperlessStatements,
        boolean pushNotificationsEnabled
    ) {
    }
}
