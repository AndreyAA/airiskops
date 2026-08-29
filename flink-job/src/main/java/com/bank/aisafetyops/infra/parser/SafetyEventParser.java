package com.bank.aisafetyops.infra.parser;

import com.bank.aisafetyops.model.EventType;
import com.bank.aisafetyops.model.GuardrailNames;
import com.bank.aisafetyops.model.SafetyEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;

/**
 * Converts raw JSON payloads into normalized {@code SafetyEvent} instances.
 *
 * <p>The parser enforces the minimum identity and guardrail requirements needed
 * by downstream keyed state, watermark handling, and window aggregation.
 */
public final class SafetyEventParser {
    private static final int DEFAULT_TOKEN_COUNT = 0;
    private static final String FIELD_EVENT_TYPE = "eventType";
    private static final String FIELD_AGENT_ID = "agentId";
    private static final String FIELD_TENANT_ID = "tenantId";
    private static final String FIELD_ENVIRONMENT = "environment";
    private static final String FIELD_SESSION_ID = "sessionId";
    private static final String FIELD_REQUEST_ID = "requestId";
    private static final String FIELD_TURN_ID = "turnId";
    private static final String FIELD_EVENT_TIME = "eventTime";
    private static final String FIELD_MODEL_NAME = "modelName";
    private static final String FIELD_USER_ID = "userId";
    private static final String FIELD_CHANNEL = "channel";
    private static final String FIELD_INPUT_TOKENS = "inputTokens";
    private static final String FIELD_OUTPUT_TOKENS = "outputTokens";
    private static final String FIELD_GUARDRAIL_NAME = "guardrailName";
    private static final String FIELD_GUARDRAIL_VERSION = "guardrailVersion";
    private static final String FIELD_POLICY_VERSION = "policyVersion";
    private static final String FIELD_CONFIDENCE = "confidence";
    private static final String FIELD_TRIGGERED = "triggered";
    private static final String FIELD_DETECTOR_LATENCY_MS = "detectorLatencyMs";
    private static final String FIELD_DETECTOR_STATUS = "detectorStatus";

    private static final String ERROR_GUARDRAIL_REQUIRED = "Missing required field: guardrailName";
    private static final String ERROR_CONFIDENCE_REQUIRED = "Missing required confidence for confidence-based guardrail";
    private static final String ERROR_TRIGGERED_REQUIRED = "Missing required triggered field for guardrail finding";
    private static final String ERROR_UNSUPPORTED_EVENT_TYPE = "Unsupported eventType: ";
    private static final String ERROR_PARSE = "Parse error: ";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public ParseResult parse(String rawPayload) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(rawPayload);
            // The parser is intentionally strict on identity and time fields because
            // all later stateful logic depends on stable agent/session/request keys.
            String eventTypeRaw = readRequiredText(root, FIELD_EVENT_TYPE);
            EventType eventType = EventType.valueOf(eventTypeRaw);

            String agentId = readRequiredText(root, FIELD_AGENT_ID);
            String sessionId = readRequiredText(root, FIELD_SESSION_ID);
            String requestId = readRequiredText(root, FIELD_REQUEST_ID);
            long eventTimeMillis = Instant.parse(readRequiredText(root, FIELD_EVENT_TIME)).toEpochMilli();

            String guardrailName = readOptionalText(root, FIELD_GUARDRAIL_NAME);
            Double confidence = root.hasNonNull(FIELD_CONFIDENCE) ? root.get(FIELD_CONFIDENCE).asDouble() : null;
            Boolean triggered = root.hasNonNull(FIELD_TRIGGERED) ? root.get(FIELD_TRIGGERED).asBoolean() : null;

            if (eventType == EventType.GUARDRAIL_FINDING && guardrailName == null) {
                return ParseResult.invalid(ERROR_GUARDRAIL_REQUIRED, rawPayload);
            }
            // Confidence-bearing guardrails are model-score based, so missing
            // confidence means we cannot meaningfully classify or aggregate them.
            if (GuardrailNames.requiresConfidence(guardrailName)) {
                if (confidence == null) {
                    return ParseResult.invalid(ERROR_CONFIDENCE_REQUIRED, rawPayload);
                }
            }
            // Boolean guardrails, and in practice any finding event in MVP, must
            // explicitly declare whether they fired. Inferring this from absence
            // would make quality monitoring ambiguous.
            if (triggered == null && eventType == EventType.GUARDRAIL_FINDING) {
                return ParseResult.invalid(ERROR_TRIGGERED_REQUIRED, rawPayload);
            }

            SafetyEvent event = new SafetyEvent(
                    readOptionalText(root, FIELD_TENANT_ID),
                    readOptionalText(root, FIELD_ENVIRONMENT),
                    agentId,
                    sessionId,
                    requestId,
                    readOptionalText(root, FIELD_TURN_ID),
                    eventTimeMillis,
                    eventType,
                    readOptionalText(root, FIELD_MODEL_NAME),
                    readOptionalText(root, FIELD_USER_ID),
                    readOptionalText(root, FIELD_CHANNEL),
                    readOptionalInt(root, FIELD_INPUT_TOKENS),
                    readOptionalInt(root, FIELD_OUTPUT_TOKENS),
                    guardrailName,
                    readOptionalText(root, FIELD_GUARDRAIL_VERSION),
                    readOptionalText(root, FIELD_POLICY_VERSION),
                    confidence,
                    triggered,
                    root.hasNonNull(FIELD_DETECTOR_LATENCY_MS) ? root.get(FIELD_DETECTOR_LATENCY_MS).asLong() : null,
                    readOptionalText(root, FIELD_DETECTOR_STATUS),
                    rawPayload
            );
            return ParseResult.success(event);
        } catch (IllegalArgumentException e) {
            return ParseResult.invalid(ERROR_UNSUPPORTED_EVENT_TYPE + e.getMessage(), rawPayload);
        } catch (Exception e) {
            return ParseResult.invalid(ERROR_PARSE + e.getMessage(), rawPayload);
        }
    }

    private static String readRequiredText(JsonNode root, String fieldName) {
        if (!root.hasNonNull(fieldName)) {
            throw new IllegalArgumentException("Missing required field: " + fieldName);
        }
        return root.get(fieldName).asText();
    }

    private static String readOptionalText(JsonNode root, String fieldName) {
        return root.hasNonNull(fieldName) ? root.get(fieldName).asText() : null;
    }

    private static int readOptionalInt(JsonNode root, String fieldName) {
        return root.hasNonNull(fieldName) ? root.get(fieldName).asInt() : DEFAULT_TOKEN_COUNT;
    }
}
