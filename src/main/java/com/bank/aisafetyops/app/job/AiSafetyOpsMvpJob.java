package com.bank.aisafetyops.app.job;

import com.bank.aisafetyops.app.config.JobConfig;
import com.bank.aisafetyops.app.support.JobTopology;
import com.bank.aisafetyops.app.usecase.IncrementOneTopologyBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class AiSafetyOpsMvpJob {
    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        IncrementOneTopologyBuilder.configure(env, config);
        env.execute(JobTopology.JOB_NAME);
    }
}
