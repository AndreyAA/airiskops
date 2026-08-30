package com.bank.aisafetyops.model;

/**
 * Fully resolved incident policy after applying agent-specific overrides.
 */
public record EffectiveIncidentPolicy(
        String version,
        IncidentGuardrailPolicy promptInjection,
        IncidentGuardrailPolicy toxicity,
        IncidentGuardrailPolicy looping,
        IncidentGuardrailPolicy systemPromptLeakage
) {
}
