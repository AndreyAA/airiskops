package com.bank.airiskops.app.support;

import com.bank.airiskops.app.config.IncidentConfig;
import com.bank.airiskops.model.EffectiveIncidentPolicy;
import com.bank.airiskops.model.GuardrailNames;
import com.bank.airiskops.model.IncidentGuardrailPolicy;
import com.bank.airiskops.model.IncidentPolicy;
import com.bank.airiskops.model.IncidentRuleNames;
import com.bank.airiskops.model.IncidentSeverity;
import com.bank.airiskops.model.SafetyEvent;
import com.bank.airiskops.model.SessionRiskSnapshot;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure incident rule evaluator extracted from the Flink process function.
 *
 * <p>This class keeps the business correlation logic testable without requiring
 * Flink operator harness serialization for every rule combination.
 */
public final class IncidentEmissionPlanner {
    private final IncidentConfig config;
    private final IncidentPolicy policy;

    public IncidentEmissionPlanner(IncidentConfig config, IncidentPolicy policy) {
        this.config = config;
        this.policy = policy;
    }

    public List<PlannedIncidentEmission> plan(SessionRiskSnapshot snapshot, SafetyEvent event) {
        List<PlannedIncidentEmission> emissions = new ArrayList<>();
        EffectiveIncidentPolicy effectivePolicy = resolveEffectivePolicy(event.agentId());

        if (GuardrailNames.PROMPT_INJECTION.equals(event.guardrailName())) {
            addPromptInjectionBurstIfNeeded(snapshot, emissions, effectivePolicy);
            addLeakageWithInjectionIfNeeded(snapshot, emissions, effectivePolicy);
            return emissions;
        }
        if (GuardrailNames.TOXICITY.equals(event.guardrailName())) {
            addToxicityCampaignIfNeeded(snapshot, emissions, effectivePolicy);
            return emissions;
        }
        if (GuardrailNames.LOOPING.equals(event.guardrailName())) {
            addLoopingPersistenceIfNeeded(snapshot, emissions, effectivePolicy);
            return emissions;
        }
        if (GuardrailNames.SYSTEM_PROMPT_LEAKAGE.equals(event.guardrailName())) {
            addLeakageWithInjectionIfNeeded(snapshot, emissions, effectivePolicy);
        }
        return emissions;
    }

    private void addPromptInjectionBurstIfNeeded(
            SessionRiskSnapshot snapshot,
            List<PlannedIncidentEmission> emissions,
            EffectiveIncidentPolicy effectivePolicy
    ) {
        if (snapshot.promptInjectionCount() < config.promptInjectionBurstMinFindings()) {
            return;
        }
        boolean update = snapshot.promptInjectionBurstEmitted();
        if (update && !config.emitUpdates()) {
            return;
        }
        IncidentSeverity severity = snapshot.maxPromptInjectionConfidence() >= promptInjectionHighThreshold(effectivePolicy)
                ? IncidentSeverity.HIGH
                : IncidentSeverity.MEDIUM;
        emissions.add(new PlannedIncidentEmission(
                IncidentRuleNames.PROMPT_INJECTION_BURST,
                severity,
                snapshot.incrementPromptInjectionBurstRevision(),
                update,
                effectivePolicy.version(),
                "Prompt injection burst detected: findings="
                        + snapshot.promptInjectionCount()
                        + ", maxConfidence="
                        + formatConfidence(snapshot.maxPromptInjectionConfidence())
        ));
    }

    private void addToxicityCampaignIfNeeded(
            SessionRiskSnapshot snapshot,
            List<PlannedIncidentEmission> emissions,
            EffectiveIncidentPolicy effectivePolicy
    ) {
        if (snapshot.toxicityCount() < config.toxicityCampaignMinFindings()) {
            return;
        }
        boolean update = snapshot.toxicityCampaignEmitted();
        if (update && !config.emitUpdates()) {
            return;
        }
        IncidentSeverity severity = snapshot.maxToxicityConfidence() >= toxicityHighThreshold(effectivePolicy)
                ? IncidentSeverity.HIGH
                : IncidentSeverity.MEDIUM;
        emissions.add(new PlannedIncidentEmission(
                IncidentRuleNames.TOXICITY_CAMPAIGN,
                severity,
                snapshot.incrementToxicityCampaignRevision(),
                update,
                effectivePolicy.version(),
                "Toxicity campaign detected: findings="
                        + snapshot.toxicityCount()
                        + ", maxConfidence="
                        + formatConfidence(snapshot.maxToxicityConfidence())
        ));
    }

    private void addLeakageWithInjectionIfNeeded(
            SessionRiskSnapshot snapshot,
            List<PlannedIncidentEmission> emissions,
            EffectiveIncidentPolicy effectivePolicy
    ) {
        if (snapshot.promptInjectionCount() == 0 || snapshot.systemPromptLeakageCount() == 0) {
            return;
        }
        boolean update = snapshot.leakageWithInjectionEmitted();
        if (update && !config.emitUpdates()) {
            return;
        }
        IncidentSeverity severity = snapshot.maxPromptInjectionConfidence() >= promptInjectionCriticalThreshold(effectivePolicy)
                ? IncidentSeverity.CRITICAL
                : IncidentSeverity.HIGH;
        emissions.add(new PlannedIncidentEmission(
                IncidentRuleNames.LEAKAGE_WITH_INJECTION,
                severity,
                snapshot.incrementLeakageWithInjectionRevision(),
                update,
                effectivePolicy.version(),
                "Prompt injection combined with system prompt leakage: promptInjectionFindings="
                        + snapshot.promptInjectionCount()
                        + ", leakageFindings="
                        + snapshot.systemPromptLeakageCount()
        ));
    }

    private void addLoopingPersistenceIfNeeded(
            SessionRiskSnapshot snapshot,
            List<PlannedIncidentEmission> emissions,
            EffectiveIncidentPolicy effectivePolicy
    ) {
        if (snapshot.loopingCount() < config.loopingMinOccurrences()) {
            return;
        }
        boolean update = snapshot.loopingPersistenceEmitted();
        if (update && !config.emitUpdates()) {
            return;
        }
        IncidentSeverity severity = loopingSeverity(effectivePolicy, snapshot.loopingCount() >= config.loopingMinOccurrences() * 2);
        emissions.add(new PlannedIncidentEmission(
                IncidentRuleNames.LOOPING_PERSISTENCE,
                severity,
                snapshot.incrementLoopingPersistenceRevision(),
                update,
                effectivePolicy.version(),
                "Looping persistence detected: occurrences=" + snapshot.loopingCount()
        ));
    }

    private EffectiveIncidentPolicy resolveEffectivePolicy(String agentId) {
        return policy == null ? fallbackPolicy() : policy.resolveForAgent(agentId);
    }

    private static EffectiveIncidentPolicy fallbackPolicy() {
        return new EffectiveIncidentPolicy(
                "config-fallback",
                new IncidentGuardrailPolicy(null, 0.90d, 0.90d, null),
                new IncidentGuardrailPolicy(null, 0.85d, null, null),
                new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.MEDIUM),
                new IncidentGuardrailPolicy(null, null, null, IncidentSeverity.CRITICAL)
        );
    }

    private static double promptInjectionHighThreshold(EffectiveIncidentPolicy effectivePolicy) {
        return valueOrDefault(effectivePolicy.promptInjection().high(), 0.90d);
    }

    private static double promptInjectionCriticalThreshold(EffectiveIncidentPolicy effectivePolicy) {
        return valueOrDefault(effectivePolicy.promptInjection().critical(), promptInjectionHighThreshold(effectivePolicy));
    }

    private static double toxicityHighThreshold(EffectiveIncidentPolicy effectivePolicy) {
        return valueOrDefault(effectivePolicy.toxicity().high(), 0.85d);
    }

    private static IncidentSeverity loopingSeverity(EffectiveIncidentPolicy effectivePolicy, boolean escalate) {
        IncidentSeverity baseSeverity = effectivePolicy.looping().severity() == null
                ? IncidentSeverity.MEDIUM
                : effectivePolicy.looping().severity();
        if (!escalate) {
            return baseSeverity;
        }
        return baseSeverity == IncidentSeverity.MEDIUM ? IncidentSeverity.HIGH : baseSeverity;
    }

    private static double valueOrDefault(Double value, double defaultValue) {
        return value == null ? defaultValue : value;
    }

    private static String formatConfidence(double confidence) {
        return confidence < 0 ? "n/a" : String.format(java.util.Locale.US, "%.3f", confidence);
    }

    /**
     * Immutable description of one incident emission decision.
     */
    public record PlannedIncidentEmission(
            String ruleName,
            IncidentSeverity severity,
            int revision,
            boolean update,
            String appliedPolicyVersion,
            String summary
    ) {
    }
}
