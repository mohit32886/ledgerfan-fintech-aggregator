package com.ledgerfan.aggregator.orchestrator;

import com.ledgerfan.aggregator.domain.UpstreamPayloads.CheckingAccountSummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.CreditCardPortfolio;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.CustomerPreferences;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.DebitCardControls;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.FraudRiskSignals;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.FxTreasuryRates;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.GeneralLedgerPosition;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.InvestmentWealthSummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.KycComplianceProfile;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.LoanMortgageSummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.RewardsLoyaltySummary;
import com.ledgerfan.aggregator.domain.UpstreamPayloads.StandingOrdersSummary;

/**
 * Unified 12-domain financial profile returned by the concurrent Java 21 aggregator.
 * Non-critical sections degrade gracefully to null or stale cache snapshots on upstream SLA breach.
 */
public record FinancialProfileResponse(
    AggregationMetadata metadata,
    CheckingAccountSummary checkingAccount,
    KycComplianceProfile kycCompliance,
    GeneralLedgerPosition generalLedger,
    CreditCardPortfolio creditCards,
    DebitCardControls debitCardControls,
    FraudRiskSignals fraudRiskSignals,
    LoanMortgageSummary loansAndMortgages,
    InvestmentWealthSummary investmentsAndWealth,
    RewardsLoyaltySummary rewardsAndLoyalty,
    StandingOrdersSummary standingOrders,
    FxTreasuryRates fxTreasuryRates,
    CustomerPreferences customerPreferences
) {
}
