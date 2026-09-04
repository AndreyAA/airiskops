package com.bank.airiskops.app.job;

import com.bank.airiskops.app.config.JobConfig;
import com.bank.airiskops.app.config.RuntimeStateProfileApplier;
import com.bank.airiskops.app.support.JobTopology;
import com.bank.airiskops.app.usecase.IncrementOneTopologyBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Main entry point for the local AIRiskOps Flink MVP.
 *
 * <p>The job loads runtime configuration, builds the current increment topology,
 * and submits it to the Flink execution environment using the stable job name
 * defined in {@link JobTopology}.
 */
public final class AiRiskOpsMvpJob {
    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RuntimeStateProfileApplier.apply(env, config.runtimeState());
        IncrementOneTopologyBuilder.configure(env, config);
        env.execute(JobTopology.JOB_NAME);
    }
}
