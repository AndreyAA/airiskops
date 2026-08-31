package com.bank.airiskops.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Bootstrap incident policy loaded from YAML.
 *
 * <p>The policy contains global defaults plus optional per-agent overrides.
 */
public record IncidentPolicy(
        String version,
        String updatedBy,
        String updatedAt,
        IncidentPolicyDefaults defaults,
        Map<String, AgentIncidentPolicyOverride> agents
) implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    public EffectiveIncidentPolicy resolveForAgent(String agentId) {
        AgentIncidentPolicyOverride override = agents == null ? null : agents.get(agentId);
        return new EffectiveIncidentPolicy(
                version,
                merge(defaults.promptInjection(), override == null ? null : override.promptInjection()),
                merge(defaults.toxicity(), override == null ? null : override.toxicity()),
                merge(defaults.looping(), override == null ? null : override.looping()),
                merge(defaults.systemPromptLeakage(), override == null ? null : override.systemPromptLeakage())
        );
    }

    private static IncidentGuardrailPolicy merge(
            IncidentGuardrailPolicy defaults,
            IncidentGuardrailPolicy override
    ) {
        if (override == null) {
            return defaults;
        }
        return new IncidentGuardrailPolicy(
                firstNonNull(override.medium(), defaults.medium()),
                firstNonNull(override.high(), defaults.high()),
                firstNonNull(override.critical(), defaults.critical()),
                firstNonNull(override.severity(), defaults.severity())
        );
    }

    private static <T> T firstNonNull(T first, T second) {
        return first != null ? first : second;
    }
}
