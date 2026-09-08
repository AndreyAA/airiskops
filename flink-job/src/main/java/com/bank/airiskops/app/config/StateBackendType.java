package com.bank.airiskops.app.config;

/**
 * Supported local runtime state backend modes for AIRiskOps job profiles.
 *
 * <p>The default mode intentionally means "do not override Flink runtime
 * backend selection" so the existing local MVP path can stay unchanged unless
 * an explicit profile asks for another backend.
 */
public enum StateBackendType {
    DEFAULT,
    ROCKSDB;

    public static StateBackendType fromConfigValue(String value) {
        if (value == null || value.isBlank()) {
            return DEFAULT;
        }

        return switch (value.trim().toLowerCase()) {
            case "default" -> DEFAULT;
            case "rocksdb" -> ROCKSDB;
            default -> throw new IllegalArgumentException("Unsupported state backend type: " + value);
        };
    }
}
