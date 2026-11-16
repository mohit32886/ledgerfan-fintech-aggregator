package com.ledgerfan.aggregator.observability;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configures the OpenTelemetry SDK with W3C TraceContext propagation across Java 21 Virtual Threads
 * and optional OTLP HTTP export to Grafana Tempo or Jaeger.
 */
@Configuration
public class TracingConfig {

    @Bean(destroyMethod = "close")
    public SdkTracerProvider sdkTracerProvider(
        @Value("${spring.application.name:ledgerfan-fintech-aggregator}") String serviceName,
        @Value("${ledgerfan.observability.otlp-enabled:false}") boolean otlpEnabled,
        @Value("${ledgerfan.observability.otlp-endpoint:http://localhost:4318/v1/traces}") String otlpEndpoint
    ) {
        Resource resource = Resource.getDefault().merge(
            Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), serviceName))
        );

        SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
            .setResource(resource);

        if (otlpEnabled) {
            OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
                .setEndpoint(otlpEndpoint)
                .setTimeout(Duration.ofSeconds(3))
                .build();
            builder.addSpanProcessor(BatchSpanProcessor.builder(exporter).build());
        }

        return builder.build();
    }

    @Bean
    public OpenTelemetry openTelemetry(SdkTracerProvider sdkTracerProvider) {
        return OpenTelemetrySdk.builder()
            .setTracerProvider(sdkTracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .build();
    }

    @Bean
    public Tracer ledgerFanTracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("com.ledgerfan.aggregator", "1.0.0");
    }
}
