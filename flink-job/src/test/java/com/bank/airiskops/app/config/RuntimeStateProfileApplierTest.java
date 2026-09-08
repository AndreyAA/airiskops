package com.bank.airiskops.app.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

class RuntimeStateProfileApplierTest {
    @Test
    void leavesDefaultProfileUntouched() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        RuntimeStateProfileApplier.apply(env, RuntimeStateConfig.defaults());

        assertNull(env.getStateBackend());
        assertNull(env.getDefaultSavepointDirectory());
        assertEquals(null, env.getConfiguration().toMap().get("execution.checkpointing.dir"));
    }

    @Test
    void appliesRocksDbProfileToExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RuntimeStateConfig config = new RuntimeStateConfig(
                StateBackendType.ROCKSDB,
                true,
                "file:///opt/flink/state/checkpoints",
                "file:///opt/flink/state/savepoints",
                "/opt/flink/state/rocksdb"
        );

        RuntimeStateProfileApplier.apply(env, config);

        assertInstanceOf(EmbeddedRocksDBStateBackend.class, env.getStateBackend());
        assertEquals(
                "file:/opt/flink/state/savepoints",
                env.getDefaultSavepointDirectory().toString()
        );
        assertEquals(
                "file:///opt/flink/state/checkpoints",
                env.getConfiguration().toMap().get("execution.checkpointing.dir")
        );
        assertEquals(
                "/opt/flink/state/rocksdb",
                env.getConfiguration().toMap().get("state.backend.rocksdb.localdir")
        );
    }
}
