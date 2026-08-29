package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.infra.serde.JsonSerde;
import com.bank.aisafetyops.model.GuardrailWindowAggregate;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Serializes window aggregates to JSON before they are written to Kafka.
 *
 * <p>Serialization stays isolated from the aggregation operators so transport
 * format changes do not force rewrites in business logic.
 */
public final class SerializeGuardrailAggregateFunction implements MapFunction<GuardrailWindowAggregate, String> {
    @Override
    public String map(GuardrailWindowAggregate value) {
        return JsonSerde.toJson(value);
    }
}
