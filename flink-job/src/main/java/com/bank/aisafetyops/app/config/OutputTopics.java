package com.bank.aisafetyops.app.config;

/**
 * Output Kafka topics used by the AISafetyOps MVP pipeline.
 *
 * <p>The record keeps downstream topic names grouped together so topology code
 * can stay explicit without hardcoding topic literals in multiple operators.
 */
public record OutputTopics(
        String normalizedEventsTopic,
        String invalidEventsTopic,
        String lateEventsTopic,
        String guardrailAggregatesTopic,
        String basicIncidentsTopic
) {
}
