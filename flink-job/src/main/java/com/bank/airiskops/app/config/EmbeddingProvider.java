package com.bank.airiskops.app.config;

/** Selects the explicitly configured local evidence embedding implementation. */
public enum EmbeddingProvider {
    DETERMINISTIC,
    LANGCHAIN4J_ONNX;

    public static EmbeddingProvider fromConfig(String value) {
        return switch (value.trim().toLowerCase().replace('-', '_')) {
            case "deterministic" -> DETERMINISTIC;
            case "langchain4j_onnx" -> LANGCHAIN4J_ONNX;
            default -> throw new IllegalArgumentException("Unsupported embedding provider: " + value);
        };
    }
}
