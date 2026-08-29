package com.bank.aisafetyops.model;

/**
 * Grouping key for guardrail aggregation windows.
 *
 * <p>The key captures the operational dimensions currently used to analyze
 * guardrail findings in Kafka outputs and Grafana dashboards.
 */
public record GuardrailAggregateKey(
        String agentId,
        String guardrailName,
        String guardrailVersion,
        String policyVersion,
        String modelName
) {
}
