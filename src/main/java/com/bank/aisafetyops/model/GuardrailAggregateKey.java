package com.bank.aisafetyops.model;

public record GuardrailAggregateKey(
        String agentId,
        String guardrailName,
        String guardrailVersion,
        String policyVersion,
        String modelName
) {
}
