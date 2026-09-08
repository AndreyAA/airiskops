package com.bank.airiskops.app.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.bank.airiskops.app.config.PipelineDeliveryGuarantee;
import com.bank.airiskops.app.config.RuntimeContractConfig;
import com.bank.airiskops.app.config.RuntimeStateConfig;
import com.bank.airiskops.app.config.StateBackendType;
import com.bank.airiskops.model.EventType;
import com.bank.airiskops.model.SafetyEvent;
import java.time.Duration;
import java.util.List;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class RuntimeContractMetricsFunctionTest {
    @Test
    void passesEventsThroughWhileRegisteringRuntimeContractMetrics() throws Exception {
        RuntimeContractMetricsFunction function = new RuntimeContractMetricsFunction(
                new RuntimeContractConfig(
                        "tumbling-event-time",
                        List.of(Duration.ofMinutes(1), Duration.ofMinutes(5)),
                        PipelineDeliveryGuarantee.AT_LEAST_ONCE
                ),
                new RuntimeStateConfig(
                        StateBackendType.ROCKSDB,
                        true,
                        "file:///opt/flink/state/checkpoints",
                        "file:///opt/flink/state/savepoints",
                        "/opt/flink/state/rocksdb"
                ),
                Duration.ofSeconds(30),
                Duration.ofMinutes(5),
                Duration.ofMinutes(1),
                Duration.ofSeconds(20),
                Duration.ofSeconds(3)
        );

        try (OneInputStreamOperatorTestHarness<SafetyEvent, SafetyEvent> harness =
                     new OneInputStreamOperatorTestHarness<>(new StreamMap<>(function))) {
            harness.open();
            SafetyEvent event = buildEvent(1_788_307_200_000L);

            harness.processElement(event, event.eventTimeMillis());

            assertEquals(List.of(event), harness.extractOutputValues());
        }
    }

    private static SafetyEvent buildEvent(long eventTimeMillis) {
        return new SafetyEvent(
                "tenant-1",
                "local",
                "agent-1",
                "session-1",
                "req-1",
                "turn-1",
                eventTimeMillis,
                EventType.GUARDRAIL_FINDING,
                "gpt-4.1-mini",
                "user-1",
                "web",
                100,
                50,
                "PROMPT_INJECTION",
                "pi-v1",
                "policy-v1",
                0.88d,
                true,
                12L,
                "OK",
                "{}"
        );
    }
}
