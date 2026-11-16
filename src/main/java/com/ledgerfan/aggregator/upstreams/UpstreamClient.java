package com.ledgerfan.aggregator.upstreams;

import com.ledgerfan.aggregator.domain.UpstreamCriticality;
import com.ledgerfan.aggregator.domain.UpstreamProtocol;

import java.time.Duration;

/**
 * Contract for each downstream banking capability queried during financial profile aggregation.
 */
public interface UpstreamClient<T> {

    String name();

    UpstreamProtocol protocol();

    UpstreamCriticality criticality();

    Class<T> payloadType();

    Duration slaTimeout();

    T fetch(String accountId);
}
