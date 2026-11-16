package com.ledgerfan.aggregator.orchestrator;

import com.ledgerfan.aggregator.domain.UpstreamCallOutcome;
import io.opentelemetry.context.Context;

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.StructuredTaskScope;

/**
 * Custom Java 21 {@link StructuredTaskScope} configured for partial-degradation fan-out:
 * individual upstream failures or timeouts never cancel healthy sibling calls, and
 * OpenTelemetry W3C {@link Context} is propagated into every forked Virtual Thread.
 */
@SuppressWarnings("preview")
public class PartialDegradationTaskScope extends StructuredTaskScope<UpstreamCallOutcome<?>> {

    private final Context parentOtelContext;
    private final Queue<UpstreamCallOutcome<?>> completedOutcomes = new ConcurrentLinkedQueue<>();
    private final Queue<Throwable> unexpectedExceptions = new ConcurrentLinkedQueue<>();

    public PartialDegradationTaskScope(Context parentOtelContext) {
        super("ledgerfan-aggregator", Thread.ofVirtual().name("vt-fanout-", 0).factory());
        this.parentOtelContext = parentOtelContext;
    }

    public <U extends UpstreamCallOutcome<?>> Subtask<U> forkWithOtelContext(Callable<U> task) {
        return super.fork(parentOtelContext.wrap(task));
    }

    @Override
    protected void handleComplete(Subtask<? extends UpstreamCallOutcome<?>> subtask) {
        switch (subtask.state()) {
            case SUCCESS -> {
                UpstreamCallOutcome<?> outcome = subtask.get();
                if (outcome != null) {
                    completedOutcomes.add(outcome);
                }
            }
            case FAILED -> unexpectedExceptions.add(subtask.exception());
            case UNAVAILABLE -> {
                // Subtask did not complete before joinUntil deadline; handled gracefully by caller.
            }
        }
    }

    public Collection<UpstreamCallOutcome<?>> completedOutcomes() {
        return completedOutcomes;
    }

    public Collection<Throwable> unexpectedExceptions() {
        return unexpectedExceptions;
    }
}
