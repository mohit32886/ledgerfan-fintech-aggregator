package com.ledgerfan.aggregator.domain;

/**
 * Business criticality tier governing partial degradation behavior during fan-out aggregation.
 */
public enum UpstreamCriticality {
    CRITICAL,
    IMPORTANT,
    DEGRADABLE
}
