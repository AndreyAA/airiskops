package com.bank.airiskops.app.functions;

import com.bank.airiskops.model.DetectorStatuses;
import com.bank.airiskops.model.EventType;
import com.bank.airiskops.model.GuardrailNames;
import com.bank.airiskops.model.SafetyEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Mutable accumulator for guardrail window aggregation.
 *
 * <p>The accumulator combines raw finding counts, trigger counts, token usage,
 * confidence statistics, and detector latency/error signals before the final
 * window result is materialized as {@code GuardrailWindowAggregate}.
 */
public final class GuardrailAggregateAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tenantId;
    private String agentId;
    private String guardrailName;
    private String guardrailVersion;
    private String policyVersion;
    private String modelName;
    private long totalEvents;
    private long guardrailFindingCount;
    private long triggeredCount;
    private long loopingTriggeredCount;
    private long systemPromptLeakageTriggeredCount;
    private long inputTokens;
    private long outputTokens;
    private long detectorErrorCount;
    private long detectorLatencyCount;
    private long detectorLatencySum;
    private Long minDetectorLatencyMs;
    private Long maxDetectorLatencyMs;
    private long latestEventTimeMillis;
    private long confidenceCount;
    private double confidenceSum;
    private Double minConfidence;
    private Double maxConfidence;
    private final List<Double> confidenceValues = new ArrayList<>();
    private final List<Double> triggeredConfidenceValues = new ArrayList<>();

    public void add(SafetyEvent event) {
        tenantId = firstNonBlank(tenantId, event.tenantId());
        agentId = firstNonBlank(agentId, event.agentId());
        guardrailName = firstNonBlank(guardrailName, event.guardrailName());
        guardrailVersion = firstNonBlank(guardrailVersion, event.guardrailVersion());
        policyVersion = firstNonBlank(policyVersion, event.policyVersion());
        modelName = firstNonBlank(modelName, event.modelName());

        totalEvents++;
        inputTokens += event.inputTokens();
        outputTokens += event.outputTokens();
        latestEventTimeMillis = Math.max(latestEventTimeMillis, event.eventTimeMillis());

        if (event.eventType() == EventType.GUARDRAIL_FINDING) {
            guardrailFindingCount++;
        }
        if (Boolean.TRUE.equals(event.triggered())) {
            triggeredCount++;
            if (GuardrailNames.LOOPING.equals(event.guardrailName())) {
                loopingTriggeredCount++;
            }
            if (GuardrailNames.SYSTEM_PROMPT_LEAKAGE.equals(event.guardrailName())) {
                systemPromptLeakageTriggeredCount++;
            }
        }
        if (!DetectorStatuses.isOk(event.detectorStatus())) {
            detectorErrorCount++;
        }
        if (event.detectorLatencyMs() != null) {
            detectorLatencyCount++;
            detectorLatencySum += event.detectorLatencyMs();
            minDetectorLatencyMs = minLong(minDetectorLatencyMs, event.detectorLatencyMs());
            maxDetectorLatencyMs = maxLong(maxDetectorLatencyMs, event.detectorLatencyMs());
        }
        if (event.confidence() != null) {
            confidenceCount++;
            confidenceSum += event.confidence();
            minConfidence = minDouble(minConfidence, event.confidence());
            maxConfidence = maxDouble(maxConfidence, event.confidence());
            confidenceValues.add(event.confidence());
            if (Boolean.TRUE.equals(event.triggered())) {
                triggeredConfidenceValues.add(event.confidence());
            }
        }
    }

    public GuardrailAggregateAccumulator merge(GuardrailAggregateAccumulator other) {
        tenantId = firstNonBlank(tenantId, other.tenantId);
        agentId = firstNonBlank(agentId, other.agentId);
        guardrailName = firstNonBlank(guardrailName, other.guardrailName);
        guardrailVersion = firstNonBlank(guardrailVersion, other.guardrailVersion);
        policyVersion = firstNonBlank(policyVersion, other.policyVersion);
        modelName = firstNonBlank(modelName, other.modelName);
        totalEvents += other.totalEvents;
        guardrailFindingCount += other.guardrailFindingCount;
        triggeredCount += other.triggeredCount;
        loopingTriggeredCount += other.loopingTriggeredCount;
        systemPromptLeakageTriggeredCount += other.systemPromptLeakageTriggeredCount;
        inputTokens += other.inputTokens;
        outputTokens += other.outputTokens;
        detectorErrorCount += other.detectorErrorCount;
        detectorLatencyCount += other.detectorLatencyCount;
        detectorLatencySum += other.detectorLatencySum;
        minDetectorLatencyMs = minLong(minDetectorLatencyMs, other.minDetectorLatencyMs);
        maxDetectorLatencyMs = maxLong(maxDetectorLatencyMs, other.maxDetectorLatencyMs);
        latestEventTimeMillis = Math.max(latestEventTimeMillis, other.latestEventTimeMillis);
        confidenceCount += other.confidenceCount;
        confidenceSum += other.confidenceSum;
        minConfidence = minDouble(minConfidence, other.minConfidence);
        maxConfidence = maxDouble(maxConfidence, other.maxConfidence);
        confidenceValues.addAll(other.confidenceValues);
        triggeredConfidenceValues.addAll(other.triggeredConfidenceValues);
        return this;
    }

    public String tenantId() {
        return tenantId;
    }

    public String agentId() {
        return agentId;
    }

    public long totalEvents() {
        return totalEvents;
    }

    public long guardrailFindingCount() {
        return guardrailFindingCount;
    }

    public long triggeredCount() {
        return triggeredCount;
    }

    public long loopingTriggeredCount() {
        return loopingTriggeredCount;
    }

    public long systemPromptLeakageTriggeredCount() {
        return systemPromptLeakageTriggeredCount;
    }

    public long inputTokens() {
        return inputTokens;
    }

    public long outputTokens() {
        return outputTokens;
    }

    public long detectorErrorCount() {
        return detectorErrorCount;
    }

    public Double minConfidence() {
        return minConfidence;
    }

    public Double avgConfidence() {
        return confidenceCount == 0 ? null : confidenceSum / confidenceCount;
    }

    public Double maxConfidence() {
        return maxConfidence;
    }

    public Long minDetectorLatencyMs() {
        return minDetectorLatencyMs;
    }

    public Double avgDetectorLatencyMs() {
        return detectorLatencyCount == 0 ? null : (double) detectorLatencySum / detectorLatencyCount;
    }

    public Long maxDetectorLatencyMs() {
        return maxDetectorLatencyMs;
    }

    public long detectorLatencyCount() {
        return detectorLatencyCount;
    }

    public long latestEventTimeMillis() {
        return latestEventTimeMillis;
    }

    public List<Double> confidenceValues() {
        return List.copyOf(confidenceValues);
    }

    public List<Double> triggeredConfidenceValues() {
        return List.copyOf(triggeredConfidenceValues);
    }

    public long confidenceCount() {
        return confidenceCount;
    }

    private static String firstNonBlank(String left, String right) {
        if (left != null && !left.isBlank()) {
            return left;
        }
        return right;
    }

    private static Long minLong(Long left, Long right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return Math.min(left, right);
    }

    private static Long maxLong(Long left, Long right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return Math.max(left, right);
    }

    private static Double minDouble(Double left, Double right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return Math.min(left, right);
    }

    private static Double maxDouble(Double left, Double right) {
        if (left == null) {
            return right;
        }
        if (right == null) {
            return left;
        }
        return Math.max(left, right);
    }
}
