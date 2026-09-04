package com.bank.airiskops.app.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Applies runtime state backend profile settings to a Flink execution
 * environment before the AIRiskOps topology is assembled.
 *
 * <p>This keeps backend/storage selection encapsulated outside the topology
 * builder, so switching local profiles does not affect event processing logic.
 */
public final class RuntimeStateProfileApplier {
    private static final String CHECKPOINT_STORAGE_KEY = "execution.checkpointing.storage";
    private static final String CHECKPOINT_DIR_KEY = "execution.checkpointing.dir";
    private static final String SAVEPOINT_DIR_KEY = "execution.checkpointing.savepoint-dir";
    private static final String ROCKSDB_LOCAL_DIR_KEY = "state.backend.rocksdb.localdir";

    private RuntimeStateProfileApplier() {
    }

    public static void apply(StreamExecutionEnvironment env, RuntimeStateConfig config) {
        Configuration runtimeConfiguration = new Configuration();

        if (config.checkpointsDir() != null) {
            runtimeConfiguration.setString(CHECKPOINT_STORAGE_KEY, "filesystem");
            runtimeConfiguration.setString(CHECKPOINT_DIR_KEY, config.checkpointsDir());
            env.getCheckpointConfig().setCheckpointStorage(config.checkpointsDir());
        }
        if (config.savepointsDir() != null) {
            runtimeConfiguration.setString(SAVEPOINT_DIR_KEY, config.savepointsDir());
            env.setDefaultSavepointDirectory(config.savepointsDir());
        }
        if (config.usesRocksDb()) {
            runtimeConfiguration.setString(ROCKSDB_LOCAL_DIR_KEY, config.rocksdbLocalDir());
            EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend(
                    config.incrementalCheckpointsEnabled()
            ).configure(runtimeConfiguration, RuntimeStateProfileApplier.class.getClassLoader());
            env.setStateBackend(backend);
        }

        if (!runtimeConfiguration.toMap().isEmpty()) {
            env.configure(runtimeConfiguration);
        }
    }
}
