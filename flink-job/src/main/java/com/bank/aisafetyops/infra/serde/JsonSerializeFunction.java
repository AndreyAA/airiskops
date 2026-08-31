package com.bank.aisafetyops.infra.serde;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Serializes arbitrary pipeline records to JSON before they leave Flink.
 *
 * <p>The dedicated Flink function keeps operator typing explicit for branches
 * that emit domain-specific records such as incidents and quality metrics.
 */
public final class JsonSerializeFunction<T> implements MapFunction<T, String> {
    private static final long serialVersionUID = 1L;

    @Override
    public String map(T value) {
        return JsonSerde.toJson(value);
    }
}
