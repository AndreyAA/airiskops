package com.bank.aisafetyops.model;

/**
 * Agent-scoped incident policy overrides layered on top of global defaults.
 */
public record AgentIncidentPolicyOverride(
        IncidentGuardrailPolicy promptInjection,
        IncidentGuardrailPolicy toxicity,
        IncidentGuardrailPolicy looping,
        IncidentGuardrailPolicy systemPromptLeakage
) {
}
