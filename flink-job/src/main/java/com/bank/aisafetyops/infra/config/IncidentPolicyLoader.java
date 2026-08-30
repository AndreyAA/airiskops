package com.bank.aisafetyops.infra.config;

import com.bank.aisafetyops.model.AgentIncidentPolicyOverride;
import com.bank.aisafetyops.model.IncidentGuardrailPolicy;
import com.bank.aisafetyops.model.IncidentPolicy;
import com.bank.aisafetyops.model.IncidentPolicyDefaults;
import com.bank.aisafetyops.model.IncidentSeverity;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Loads the incident bootstrap policy from YAML.
 *
 * <p>The loader accepts partial agent overrides so local policy tuning can
 * change only the fields that matter for a particular agent rollout.
 */
public final class IncidentPolicyLoader {
    private static final String FIELD_VERSION = "version";
    private static final String FIELD_UPDATED_BY = "updatedBy";
    private static final String FIELD_UPDATED_AT = "updatedAt";
    private static final String FIELD_DEFAULTS = "defaults";
    private static final String FIELD_AGENTS = "agents";
    private static final String FIELD_PROMPT_INJECTION = "promptInjection";
    private static final String FIELD_TOXICITY = "toxicity";
    private static final String FIELD_LOOPING = "looping";
    private static final String FIELD_SYSTEM_PROMPT_LEAKAGE = "systemPromptLeakage";
    private static final String FIELD_MEDIUM = "medium";
    private static final String FIELD_HIGH = "high";
    private static final String FIELD_CRITICAL = "critical";
    private static final String FIELD_SEVERITY = "severity";

    private IncidentPolicyLoader() {
    }

    public static IncidentPolicy loadRequired(Path path) {
        return toIncidentPolicy(YamlJobConfigLoader.loadRequired(path), path);
    }

    public static IncidentPolicy loadIfExists(Path path) {
        Map<String, Object> yaml = YamlJobConfigLoader.loadIfExists(path);
        return yaml.isEmpty() ? null : toIncidentPolicy(yaml, path);
    }

    public static IncidentPolicy loadFromYamlString(String yamlContent) {
        return toIncidentPolicy(
                YamlJobConfigLoader.loadFromString(normalizeYamlMessage(yamlContent), "policy-update-message"),
                Path.of("policy-update-message")
        );
    }

    @SuppressWarnings("unchecked")
    private static IncidentPolicy toIncidentPolicy(Map<String, Object> yaml, Path path) {
        Map<String, Object> defaultsMap = requireMap(yaml, FIELD_DEFAULTS, path);
        Map<String, Object> agentsMap = readMap(yaml.get(FIELD_AGENTS));
        Map<String, AgentIncidentPolicyOverride> overrides = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : agentsMap.entrySet()) {
            Map<String, Object> overrideMap = readMap(entry.getValue());
            overrides.put(entry.getKey(), new AgentIncidentPolicyOverride(
                    readGuardrailPolicy(overrideMap, FIELD_PROMPT_INJECTION),
                    readGuardrailPolicy(overrideMap, FIELD_TOXICITY),
                    readGuardrailPolicy(overrideMap, FIELD_LOOPING),
                    readGuardrailPolicy(overrideMap, FIELD_SYSTEM_PROMPT_LEAKAGE)
            ));
        }

        return new IncidentPolicy(
                stringValue(yaml.get(FIELD_VERSION)),
                stringValue(yaml.get(FIELD_UPDATED_BY)),
                stringValue(yaml.get(FIELD_UPDATED_AT)),
                new IncidentPolicyDefaults(
                        requireGuardrailPolicy(defaultsMap, FIELD_PROMPT_INJECTION, path),
                        requireGuardrailPolicy(defaultsMap, FIELD_TOXICITY, path),
                        requireGuardrailPolicy(defaultsMap, FIELD_LOOPING, path),
                        requireGuardrailPolicy(defaultsMap, FIELD_SYSTEM_PROMPT_LEAKAGE, path)
                ),
                Collections.unmodifiableMap(overrides)
        );
    }

    private static IncidentGuardrailPolicy requireGuardrailPolicy(
            Map<String, Object> parent,
            String fieldName,
            Path path
    ) {
        return toGuardrailPolicy(requireMap(parent, fieldName, path));
    }

    private static IncidentGuardrailPolicy readGuardrailPolicy(
            Map<String, Object> parent,
            String fieldName
    ) {
        Object value = parent.get(fieldName);
        return value == null ? null : toGuardrailPolicy(readMap(value));
    }

    private static IncidentGuardrailPolicy toGuardrailPolicy(Map<String, Object> yaml) {
        return new IncidentGuardrailPolicy(
                doubleValue(yaml.get(FIELD_MEDIUM)),
                doubleValue(yaml.get(FIELD_HIGH)),
                doubleValue(yaml.get(FIELD_CRITICAL)),
                severityValue(yaml.get(FIELD_SEVERITY))
        );
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> requireMap(Map<String, Object> parent, String fieldName, Path path) {
        Object value = parent.get(fieldName);
        if (!(value instanceof Map<?, ?> mapValue)) {
            throw new IllegalArgumentException("Policy field must be a YAML map: " + fieldName + " in " + path);
        }
        return (Map<String, Object>) mapValue;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> readMap(Object value) {
        if (value == null) {
            return Collections.emptyMap();
        }
        if (!(value instanceof Map<?, ?> mapValue)) {
            throw new IllegalArgumentException("Policy field must be a YAML map");
        }
        return (Map<String, Object>) mapValue;
    }

    private static String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Double doubleValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }

    private static IncidentSeverity severityValue(Object value) {
        return value == null ? null : IncidentSeverity.valueOf(String.valueOf(value).toUpperCase());
    }

    private static String normalizeYamlMessage(String yamlContent) {
        return yamlContent == null ? "" : yamlContent.replace("\\n", "\n");
    }
}
