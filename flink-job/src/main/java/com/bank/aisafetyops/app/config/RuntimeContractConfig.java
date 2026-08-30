package com.bank.aisafetyops.app.config;

import com.bank.aisafetyops.model.WindowNames;
import java.time.Duration;
import java.util.List;

/**
 * Explicit runtime contract for AISafetyOps NRTP processing.
 *
 * <p>This record describes the processing semantics that materially affect
 * operator behavior and business interpretation: window type, configured
 * aggregate windows, and sink delivery guarantees.
 */
public record RuntimeContractConfig(
        String windowType,
        List<Duration> aggregateWindows,
        PipelineDeliveryGuarantee deliveryGuarantee
) {
    public RuntimeContractConfig {
        if (windowType == null || windowType.isBlank()) {
            throw new IllegalArgumentException("windowType must not be blank");
        }
        if (aggregateWindows == null || aggregateWindows.isEmpty()) {
            throw new IllegalArgumentException("aggregateWindows must not be empty");
        }
        if (deliveryGuarantee == null) {
            throw new IllegalArgumentException("deliveryGuarantee must not be null");
        }
    }

    public List<String> aggregateWindowNames() {
        return aggregateWindows.stream().map(WindowNames::forDuration).toList();
    }

    public String aggregateWindowsLabel() {
        return String.join(",", aggregateWindowNames());
    }

    public String metricWindowType() {
        return normalizeLabel(windowType);
    }

    public static String normalizeLabel(String value) {
        return value.toLowerCase().replace('-', '_').replace(' ', '_');
    }
}
