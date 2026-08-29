package com.bank.aisafetyops.app.config;

public record OutputTopics(
        String normalizedEventsTopic,
        String invalidEventsTopic,
        String lateEventsTopic,
        String guardrailAggregatesTopic
) {
}
