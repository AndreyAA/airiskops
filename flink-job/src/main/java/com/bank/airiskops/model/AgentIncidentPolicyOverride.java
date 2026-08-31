package com.bank.airiskops.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Agent-scoped incident policy overrides layered on top of global defaults.
 */
public record AgentIncidentPolicyOverride(
        IncidentGuardrailPolicy promptInjection,
        IncidentGuardrailPolicy toxicity,
        IncidentGuardrailPolicy looping,
        IncidentGuardrailPolicy systemPromptLeakage
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
