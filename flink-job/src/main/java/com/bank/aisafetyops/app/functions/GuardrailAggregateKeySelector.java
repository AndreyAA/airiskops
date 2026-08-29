package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.model.GuardrailAggregateKey;
import com.bank.aisafetyops.model.SafetyEvent;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Builds the logical key for guardrail aggregation windows.
 *
 * <p>The current key groups findings by agent, guardrail identity, policy
 * version, and model so that dashboards and Kafka aggregates reflect the
 * relevant operational risk dimensions.
 */
public final class GuardrailAggregateKeySelector implements KeySelector<SafetyEvent, GuardrailAggregateKey> {
    @Override
    public GuardrailAggregateKey getKey(SafetyEvent event) {
        return new GuardrailAggregateKey(
                event.agentId(),
                event.guardrailName(),
                event.guardrailVersion(),
                event.policyVersion(),
                event.modelName()
        );
    }
}
