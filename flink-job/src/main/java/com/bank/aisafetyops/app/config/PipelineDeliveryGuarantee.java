package com.bank.aisafetyops.app.config;

import org.apache.flink.connector.base.DeliveryGuarantee;

/**
 * Supported Kafka sink delivery guarantees for the AISafetyOps MVP runtime.
 *
 * <p>The current local profile is validated primarily with {@code AT_LEAST_ONCE},
 * but the enum keeps the runtime contract explicit in configuration, metrics,
 * and documentation instead of hiding it inside sink factory code.
 */
public enum PipelineDeliveryGuarantee {
    NONE(DeliveryGuarantee.NONE),
    AT_LEAST_ONCE(DeliveryGuarantee.AT_LEAST_ONCE);

    private final DeliveryGuarantee flinkGuarantee;

    PipelineDeliveryGuarantee(DeliveryGuarantee flinkGuarantee) {
        this.flinkGuarantee = flinkGuarantee;
    }

    public DeliveryGuarantee toFlinkDeliveryGuarantee() {
        return flinkGuarantee;
    }

    public String metricLabelValue() {
        return name().toLowerCase();
    }

    public static PipelineDeliveryGuarantee fromConfigValue(String value) {
        if (value == null || value.isBlank()) {
            return AT_LEAST_ONCE;
        }
        String normalized = value.trim().replace('-', '_').toUpperCase();
        return PipelineDeliveryGuarantee.valueOf(normalized);
    }
}
