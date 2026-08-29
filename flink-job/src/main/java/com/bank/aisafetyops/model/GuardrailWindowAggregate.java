package com.bank.aisafetyops.model;

/**
 * Immutable output contract for one emitted guardrail aggregation window.
 *
 * <p>The record is written to Kafka and also mirrored into metrics-friendly
 * counters, so it represents the business-visible aggregate produced by the job.
 */
public record GuardrailWindowAggregate(
        String tenantId,
        String agentId,
        String guardrailName,
        String guardrailVersion,
        String policyVersion,
        String modelName,
        String windowName,
        long windowStartMillis,
        long windowEndMillis,
        long totalEvents,
        long guardrailFindingCount,
        long triggeredCount,
        long loopingTriggeredCount,
        long systemPromptLeakageTriggeredCount,
        long inputTokens,
        long outputTokens,
        Double minConfidence,
        Double avgConfidence,
        Double maxConfidence,
        Long minDetectorLatencyMs,
        Double avgDetectorLatencyMs,
        Long maxDetectorLatencyMs,
        long detectorErrorCount
) {
}
