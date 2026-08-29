package com.bank.aisafetyops.infra.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Shared JSON serialization helper for domain and support payloads.
 *
 * <p>The current MVP uses JSON as transport format, and this helper keeps that
 * decision localized so a later format swap remains contained.
 */
public final class JsonSerde {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonSerde() {
    }

    public static String toJson(Object value) {
        try {
            return OBJECT_MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Failed to serialize value to JSON", e);
        }
    }
}
