package com.bank.aisafetyops.app.job;

import com.bank.aisafetyops.app.config.JobConfig;
import com.bank.aisafetyops.app.support.JobTopology;
import com.bank.aisafetyops.app.usecase.IncrementOneTopologyBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Main entry point for the local AISafetyOps Flink MVP.
 *
 * <p>The job loads runtime configuration, builds the current increment topology,
 * and submits it to the Flink execution environment using the stable job name
 * defined in {@link JobTopology}.
 */
public final class AiSafetyOpsMvpJob {
    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        IncrementOneTopologyBuilder.configure(env, config);
        env.execute(JobTopology.JOB_NAME);
    }
}
