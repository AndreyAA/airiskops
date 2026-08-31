package com.bank.airiskops.app.functions;

import com.bank.airiskops.infra.config.IncidentPolicyLoader;
import com.bank.airiskops.model.IncidentPolicy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

/**
 * Parses runtime policy updates from Kafka messages.
 *
 * <p>Policy updates are transported as YAML payloads flattened into one Kafka
 * line with escaped newlines, then reconstructed here before parsing.
 */
public final class ParseIncidentPolicyUpdateFunction extends RichFlatMapFunction<String, IncidentPolicy> {
    private static final String AIRISKOPS_GROUP = "airiskops";
    private static final String POLICY_GROUP = "policy";
    private static final String INVALID_METRIC = "invalid_updates_total";

    private transient Counter invalidUpdatesCounter;

    @Override
    public void open(Configuration parameters) {
        invalidUpdatesCounter = getRuntimeContext().getMetricGroup()
                .addGroup(AIRISKOPS_GROUP)
                .addGroup(POLICY_GROUP)
                .counter(INVALID_METRIC);
    }

    @Override
    public void flatMap(String value, Collector<IncidentPolicy> out) {
        try {
            out.collect(IncidentPolicyLoader.loadFromYamlString(value));
        } catch (IllegalArgumentException exception) {
            invalidUpdatesCounter.inc();
        }
    }
}
