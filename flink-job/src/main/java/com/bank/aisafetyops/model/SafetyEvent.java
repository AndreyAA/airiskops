package com.bank.aisafetyops.model;

public record SafetyEvent(
        String tenantId,
        String environment,
        String agentId,
        String sessionId,
        String requestId,
        String turnId,
        long eventTimeMillis,
        EventType eventType,
        String modelName,
        String userId,
        String channel,
        int inputTokens,
        int outputTokens,
        String guardrailName,
        String guardrailVersion,
        String policyVersion,
        Double confidence,
        Boolean triggered,
        Long detectorLatencyMs,
        String detectorStatus,
        String rawPayload
) {
}
