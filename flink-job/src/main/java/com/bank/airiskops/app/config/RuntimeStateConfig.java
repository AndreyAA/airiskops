package com.bank.airiskops.app.config;

import java.io.Serializable;

/**
 * Runtime-only state backend profile for the AIRiskOps Flink job.
 *
 * <p>This config isolates backend/storage concerns from business topology
 * semantics so local profiles can switch state handling modes without
 * scattering backend-specific conditionals through the pipeline code.
 */
public record RuntimeStateConfig(
        StateBackendType backendType,
        boolean incrementalCheckpointsEnabled,
        String checkpointsDir,
        String savepointsDir,
        String rocksdbLocalDir
) implements Serializable {
    public RuntimeStateConfig {
        backendType = backendType == null ? StateBackendType.DEFAULT : backendType;
        checkpointsDir = normalize(checkpointsDir);
        savepointsDir = normalize(savepointsDir);
        rocksdbLocalDir = normalize(rocksdbLocalDir);

        if (backendType != StateBackendType.ROCKSDB && incrementalCheckpointsEnabled) {
            throw new IllegalArgumentException(
                    "Incremental checkpoints are supported only for the RocksDB runtime state profile"
            );
        }
        if (backendType == StateBackendType.ROCKSDB) {
            requireNonBlank(checkpointsDir, "runtimeState.checkpointsDir");
            requireNonBlank(savepointsDir, "runtimeState.savepointsDir");
            requireNonBlank(rocksdbLocalDir, "runtimeState.rocksdbLocalDir");
        }
    }

    public static RuntimeStateConfig defaults() {
        return new RuntimeStateConfig(StateBackendType.DEFAULT, false, null, null, null);
    }

    public boolean usesRocksDb() {
        return backendType == StateBackendType.ROCKSDB;
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static void requireNonBlank(String value, String fieldName) {
        if (value == null) {
            throw new IllegalArgumentException("Missing required RocksDB runtime config: " + fieldName);
        }
    }
}
