package com.bank.airiskops.app.config;

/**
 * Output Kafka topics used by the AIRiskOps MVP pipeline.
 *
 * <p>The record keeps downstream topic names grouped together so topology code
 * can stay explicit without hardcoding topic literals in multiple operators.
 */
public record OutputTopics(
        String normalizedEventsTopic,
        String invalidEventsTopic,
        String lateEventsTopic,
        String guardrailAggregatesTopic,
        String basicIncidentsTopic,
        String guardrailQualityMetricsTopic
) {
}
