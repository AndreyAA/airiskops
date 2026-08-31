package com.bank.aisafetyops.model;

/**
 * Fully resolved incident policy after applying agent-specific overrides.
 */
import java.io.Serial;
import java.io.Serializable;

public record EffectiveIncidentPolicy(
        String version,
        IncidentGuardrailPolicy promptInjection,
        IncidentGuardrailPolicy toxicity,
        IncidentGuardrailPolicy looping,
        IncidentGuardrailPolicy systemPromptLeakage
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
}
