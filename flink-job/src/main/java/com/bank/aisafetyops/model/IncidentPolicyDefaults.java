package com.bank.aisafetyops.model;

/**
 * Default incident policy values shared by all agents unless overridden.
 */
public record IncidentPolicyDefaults(
        IncidentGuardrailPolicy promptInjection,
        IncidentGuardrailPolicy toxicity,
        IncidentGuardrailPolicy looping,
        IncidentGuardrailPolicy systemPromptLeakage
) {
}
