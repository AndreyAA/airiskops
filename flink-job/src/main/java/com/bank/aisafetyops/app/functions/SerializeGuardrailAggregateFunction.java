package com.bank.aisafetyops.app.functions;

import com.bank.aisafetyops.infra.serde.JsonSerde;
import com.bank.aisafetyops.model.GuardrailWindowAggregate;
import org.apache.flink.api.common.functions.MapFunction;

public final class SerializeGuardrailAggregateFunction implements MapFunction<GuardrailWindowAggregate, String> {
    @Override
    public String map(GuardrailWindowAggregate value) {
        return JsonSerde.toJson(value);
    }
}
