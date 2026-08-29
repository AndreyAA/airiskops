package com.bank.aisafetyops.infra.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
