package com.bank.airiskops.model;

/**
 * Normalized domain event used inside the AIRiskOps Flink pipeline.
 *
 * <p>The record unifies agent requests, agent responses, and guardrail findings
 * into a single transport-neutral model that downstream operators can process
 * with consistent keys and timestamps.
 */
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
        String evidenceSnippet,
        String rawPayload
) {
    /**
     * Compatibility constructor for producers of the pre-evidence event contract.
     */
    public SafetyEvent(
            String tenantId, String environment, String agentId, String sessionId, String requestId, String turnId,
            long eventTimeMillis, EventType eventType, String modelName, String userId, String channel,
            int inputTokens, int outputTokens, String guardrailName, String guardrailVersion, String policyVersion,
            Double confidence, Boolean triggered, Long detectorLatencyMs, String detectorStatus, String rawPayload
    ) {
        this(tenantId, environment, agentId, sessionId, requestId, turnId, eventTimeMillis, eventType, modelName,
                userId, channel, inputTokens, outputTokens, guardrailName, guardrailVersion, policyVersion,
                confidence, triggered, detectorLatencyMs, detectorStatus, null, rawPayload);
    }
}
