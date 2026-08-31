package com.bank.aisafetyops.model;

import java.io.Serial;
import java.io.Serializable;

/**
 * Default incident policy values shared by all agents unless overridden.
 */
public record IncidentPolicyDefaults(
        IncidentGuardrailPolicy promptInjection,
        IncidentGuardrailPolicy toxicity,
        IncidentGuardrailPolicy looping,
        IncidentGuardrailPolicy systemPromptLeakage
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
