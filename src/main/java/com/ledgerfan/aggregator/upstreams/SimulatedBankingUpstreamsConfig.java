package com.ledgerfan.aggregator.upstreams;

import com.ledgerfan.aggregator.domain.UpstreamCriticality;
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
import com.ledgerfan.aggregator.domain.UpstreamProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Registers all 12 federated banking upstream clients (REST + GraphQL) with deterministic
 * account-seeded payloads and configurable network/fault simulation.
 */
@Configuration
public class SimulatedBankingUpstreamsConfig {

    @Bean
    public List<UpstreamClient<?>> bankingUpstreams(
        UpstreamSimulationRegistry registry,
        @Value("${ledgerfan.aggregator.per-upstream-timeout-ms:60}") long perUpstreamTimeoutMs
    ) {
        Duration sla = Duration.ofMillis(perUpstreamTimeoutMs);

        return List.of(
            client(
                "checking-accounts-rest",
                UpstreamProtocol.REST,
                UpstreamCriticality.CRITICAL,
                CheckingAccountSummary.class,
                sla,
                registry,
                accountId -> {
                    long seed = seedFor(accountId);
                    BigDecimal current = BigDecimal.valueOf(12_500 + (seed % 45_000), 2)
                        .multiply(BigDecimal.valueOf(100));
                    BigDecimal available = current.subtract(BigDecimal.valueOf(245.50));
                    return new CheckingAccountSummary(
                        accountId,
                        "GB29BARC200384" + String.format("%08d", seed % 100_000_000L),
                        "GBP",
                        available.setScale(2, RoundingMode.HALF_UP),
                        current.setScale(2, RoundingMode.HALF_UP),
                        "ACTIVE"
                    );
                }
            ),
            client(
                "kyc-compliance-graphql",
                UpstreamProtocol.GRAPHQL,
                UpstreamCriticality.CRITICAL,
                KycComplianceProfile.class,
                sla,
                registry,
                accountId -> new KycComplianceProfile(
                    "TIER_3_FULL_EDD",
                    (int) (5 + (seedFor(accountId) % 18)),
                    false,
                    true,
                    Instant.now().minusSeconds(86_400)
                )
            ),
            client(
                "general-ledger-rest",
                UpstreamProtocol.REST,
                UpstreamCriticality.CRITICAL,
                GeneralLedgerPosition.class,
                sla,
                registry,
                accountId -> {
                    long seed = seedFor(accountId);
                    BigDecimal posted = BigDecimal.valueOf(14_200 + (seed % 30_000)).setScale(2, RoundingMode.HALF_UP);
                    BigDecimal debits = BigDecimal.valueOf(310.25);
                    BigDecimal credits = BigDecimal.valueOf(850.00);
                    return new GeneralLedgerPosition(
                        "GL-" + accountId,
                        posted,
                        debits,
                        credits,
                        posted.subtract(debits).add(credits),
                        94_000L + (seed % 5_000)
                    );
                }
            ),
            client(
                "credit-cards-graphql",
                UpstreamProtocol.GRAPHQL,
                UpstreamCriticality.IMPORTANT,
                CreditCardPortfolio.class,
                sla,
                registry,
                accountId -> {
                    BigDecimal limit = BigDecimal.valueOf(15_000.00).setScale(2, RoundingMode.HALF_UP);
                    BigDecimal bal = BigDecimal.valueOf(1_420 + (seedFor(accountId) % 2_500)).setScale(2, RoundingMode.HALF_UP);
                    double util = bal.divide(limit, 4, RoundingMode.HALF_UP).doubleValue();
                    return new CreditCardPortfolio(2, limit, bal, util, LocalDate.now().plusDays(14));
                }
            ),
            client(
                "debit-card-controls-rest",
                UpstreamProtocol.REST,
                UpstreamCriticality.IMPORTANT,
                DebitCardControls.class,
                sla,
                registry,
                accountId -> new DebitCardControls(
                    false,
                    true,
                    true,
                    BigDecimal.valueOf(500.00).setScale(2, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(5_000.00).setScale(2, RoundingMode.HALF_UP)
                )
            ),
            client(
                "fraud-risk-rest",
                UpstreamProtocol.REST,
                UpstreamCriticality.CRITICAL,
                FraudRiskSignals.class,
                sla,
                registry,
                accountId -> new FraudRiskSignals(
                    "LOW",
                    0.08,
                    0.97,
                    0,
                    false
                )
            ),
            client(
                "loan-mortgage-graphql",
                UpstreamProtocol.GRAPHQL,
                UpstreamCriticality.DEGRADABLE,
                LoanMortgageSummary.class,
                sla,
                registry,
                accountId -> new LoanMortgageSummary(
                    1,
                    BigDecimal.valueOf(242_500.00).setScale(2, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(1_485.40).setScale(2, RoundingMode.HALF_UP),
                    LocalDate.now().plusDays(12),
                    BigDecimal.valueOf(4.29)
                )
            ),
            client(
                "investment-wealth-rest",
                UpstreamProtocol.REST,
                UpstreamCriticality.DEGRADABLE,
                InvestmentWealthSummary.class,
                sla,
                registry,
                accountId -> new InvestmentWealthSummary(
                    BigDecimal.valueOf(68_420.15).setScale(2, RoundingMode.HALF_UP),
                    BigDecimal.valueOf(412.80).setScale(2, RoundingMode.HALF_UP),
                    11.42,
                    Map.of(
                        "GLOBAL_EQUITIES", BigDecimal.valueOf(0.65),
                        "SOVEREIGN_BONDS", BigDecimal.valueOf(0.25),
                        "MONEY_MARKET", BigDecimal.valueOf(0.10)
                    )
                )
            ),
            client(
                "rewards-loyalty-graphql",
                UpstreamProtocol.GRAPHQL,
                UpstreamCriticality.DEGRADABLE,
                RewardsLoyaltySummary.class,
                sla,
                registry,
                accountId -> new RewardsLoyaltySummary(
                    "PREMIER_BLACK",
                    48_250L + (seedFor(accountId) % 9_000),
                    BigDecimal.valueOf(312.60).setScale(2, RoundingMode.HALF_UP),
                    List.of("TRAVEL_3X", "GROCERY_2X", "DINING_BOOST")
                )
            ),
            client(
                "standing-orders-rest",
                UpstreamProtocol.REST,
                UpstreamCriticality.DEGRADABLE,
                StandingOrdersSummary.class,
                sla,
                registry,
                accountId -> new StandingOrdersSummary(
                    4,
                    7,
                    BigDecimal.valueOf(1_120.00).setScale(2, RoundingMode.HALF_UP),
                    LocalDate.now().plusDays(3)
                )
            ),
            client(
                "fx-treasury-rest",
                UpstreamProtocol.REST,
                UpstreamCriticality.DEGRADABLE,
                FxTreasuryRates.class,
                sla,
                registry,
                accountId -> new FxTreasuryRates(
                    "GBP",
                    Map.of(
                        "USD", BigDecimal.valueOf(1.2745),
                        "EUR", BigDecimal.valueOf(1.1782),
                        "CHF", BigDecimal.valueOf(1.1190),
                        "JPY", BigDecimal.valueOf(193.45)
                    ),
                    Instant.now().plusSeconds(60)
                )
            ),
            client(
                "customer-preferences-graphql",
                UpstreamProtocol.GRAPHQL,
                UpstreamCriticality.DEGRADABLE,
                CustomerPreferences.class,
                sla,
                registry,
                accountId -> new CustomerPreferences(
                    "en-GB",
                    "GB",
                    true,
                    true
                )
            )
        );
    }

    private static <T> UpstreamClient<T> client(
        String name,
        UpstreamProtocol protocol,
        UpstreamCriticality criticality,
        Class<T> payloadType,
        Duration slaTimeout,
        UpstreamSimulationRegistry registry,
        Function<String, T> payloadBuilder
    ) {
        return new UpstreamClient<>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public UpstreamProtocol protocol() {
                return protocol;
            }

            @Override
            public UpstreamCriticality criticality() {
                return criticality;
            }

            @Override
            public Class<T> payloadType() {
                return payloadType;
            }

            @Override
            public Duration slaTimeout() {
                return slaTimeout;
            }

            @Override
            public T fetch(String accountId) {
                registry.simulateCall(name);
                return payloadBuilder.apply(accountId);
            }
        };
    }

    private static long seedFor(String accountId) {
        return Math.abs((long) accountId.hashCode());
    }
}
