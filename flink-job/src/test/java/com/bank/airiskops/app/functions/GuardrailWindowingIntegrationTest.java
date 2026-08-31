package com.bank.airiskops.app.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.bank.airiskops.model.GuardrailAggregateKey;
import com.bank.airiskops.model.EventType;
import com.bank.airiskops.model.GuardrailNames;
import com.bank.airiskops.model.GuardrailWindowAggregate;
import com.bank.airiskops.model.SafetyEvent;
import java.util.List;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorBuilder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

class GuardrailWindowingIntegrationTest {
    private static final String TENANT_ID = "agent-risk-01";
    private static final String AGENT_ID = "agent-risk-01";
    private static final String SESSION_ID = "session-001";
    private static final String MODEL_NAME = "gpt-4.1-mini";
    private static final String POLICY_VERSION = "policy-v1";
    private static final String GUARDRAIL_VERSION = "pi-v1";
    private static final long BASE_TIME = 1_787_745_600_000L;
    private static final long FIRST_WINDOW_END = BASE_TIME + 60_000L;
    private static final long SECOND_WINDOW_END = BASE_TIME + 120_000L;

    @Test
    void emitsMinuteWindowsForBoundedGuardrailStream() throws Exception {
        WindowOperator<GuardrailAggregateKey, SafetyEvent, ?, GuardrailWindowAggregate, ?> operator =
                buildWindowOperator(Time.minutes(5));

        try (KeyedOneInputStreamOperatorTestHarness<GuardrailAggregateKey, SafetyEvent, GuardrailWindowAggregate> harness =
                     new KeyedOneInputStreamOperatorTestHarness<>(
                             operator,
                             new GuardrailAggregateKeySelector(),
                             TypeInformation.of(new TypeHint<>() {
                             })
                     )) {
            harness.open();
            harness.processElement(buildFinding("req-000001", BASE_TIME + 5_000L, 0.91d, true, 10L), BASE_TIME + 5_000L);
            harness.processElement(buildFinding("req-000002", BASE_TIME + 25_000L, 0.82d, true, 12L), BASE_TIME + 25_000L);
            harness.processElement(buildFinding("req-000003", BASE_TIME + 65_000L, 0.74d, false, 14L), BASE_TIME + 65_000L);
            harness.processElement(buildFinding("req-000004", BASE_TIME + 80_000L, 0.66d, false, 16L), BASE_TIME + 80_000L);

            harness.processWatermark(FIRST_WINDOW_END);
            harness.processWatermark(SECOND_WINDOW_END);

            List<GuardrailWindowAggregate> aggregates = harness.extractOutputStreamRecords()
                    .stream()
                    .map(record -> (GuardrailWindowAggregate) record.getValue())
                    .toList();

            assertEquals(2, aggregates.size());
            assertEquals(BASE_TIME, aggregates.get(0).windowStartMillis());
            assertEquals(FIRST_WINDOW_END, aggregates.get(0).windowEndMillis());
            assertEquals(2L, aggregates.get(0).totalEvents());
            assertEquals(2L, aggregates.get(0).triggeredCount());
            assertEquals(0.82d, aggregates.get(0).p50Confidence());
            assertEquals(0.91d, aggregates.get(0).p95Confidence());
            assertEquals(0.82d, aggregates.get(0).triggeredP50Confidence());
            assertEquals(0.91d, aggregates.get(0).triggeredP95Confidence());

            assertEquals(FIRST_WINDOW_END, aggregates.get(1).windowStartMillis());
            assertEquals(SECOND_WINDOW_END, aggregates.get(1).windowEndMillis());
            assertEquals(2L, aggregates.get(1).totalEvents());
            assertEquals(0L, aggregates.get(1).triggeredCount());
            assertEquals(0.66d, aggregates.get(1).p50Confidence());
            assertEquals(0.74d, aggregates.get(1).p95Confidence());
            assertNull(aggregates.get(1).triggeredP50Confidence());
            assertNull(aggregates.get(1).triggeredP95Confidence());
        }
    }

    @Test
    void emitsLateButStillAllowedGuardrailEventsForReplayOrdering() throws Exception {
        WindowOperator<GuardrailAggregateKey, SafetyEvent, ?, GuardrailWindowAggregate, ?> operator =
                buildWindowOperator(Time.minutes(5));

        try (KeyedOneInputStreamOperatorTestHarness<GuardrailAggregateKey, SafetyEvent, GuardrailWindowAggregate> harness =
                     new KeyedOneInputStreamOperatorTestHarness<>(
                             operator,
                             new GuardrailAggregateKeySelector(),
                             TypeInformation.of(new TypeHint<>() {
                             })
                     )) {
            harness.open();

            // This reproduces the real replay order: first other event types push
            // the watermark forward, then guardrail findings for older event-time
            // windows arrive afterwards but still within the agreed tolerance.
            harness.processWatermark(BASE_TIME + 180_000L);
            harness.processElement(buildFinding("req-000001", BASE_TIME + 5_000L, 0.91d, true, 10L), BASE_TIME + 5_000L);
            harness.processElement(buildFinding("req-000002", BASE_TIME + 25_000L, 0.82d, true, 12L), BASE_TIME + 25_000L);

            List<GuardrailWindowAggregate> aggregates = harness.extractOutputStreamRecords()
                    .stream()
                    .map(record -> (GuardrailWindowAggregate) record.getValue())
                    .toList();

            assertEquals(2, aggregates.size());
            assertEquals(BASE_TIME, aggregates.get(0).windowStartMillis());
            assertEquals(FIRST_WINDOW_END, aggregates.get(0).windowEndMillis());
            assertEquals(1L, aggregates.get(0).totalEvents());
            assertEquals(1L, aggregates.get(0).triggeredCount());
            assertEquals(0.91d, aggregates.get(0).p50Confidence());
            assertEquals(0.91d, aggregates.get(0).p95Confidence());
            assertEquals(0.91d, aggregates.get(0).triggeredP50Confidence());
            assertEquals(0.91d, aggregates.get(0).triggeredP95Confidence());

            assertEquals(BASE_TIME, aggregates.get(1).windowStartMillis());
            assertEquals(FIRST_WINDOW_END, aggregates.get(1).windowEndMillis());
            assertEquals(2L, aggregates.get(1).totalEvents());
            assertEquals(2L, aggregates.get(1).triggeredCount());
            assertEquals(0.82d, aggregates.get(1).p50Confidence());
            assertEquals(0.91d, aggregates.get(1).p95Confidence());
            assertEquals(0.82d, aggregates.get(1).triggeredP50Confidence());
            assertEquals(0.91d, aggregates.get(1).triggeredP95Confidence());
        }
    }

    @Test
    void leavesPercentilesNullForBooleanGuardrails() throws Exception {
        WindowOperator<GuardrailAggregateKey, SafetyEvent, ?, GuardrailWindowAggregate, ?> operator =
                buildWindowOperator(Time.minutes(5));

        try (KeyedOneInputStreamOperatorTestHarness<GuardrailAggregateKey, SafetyEvent, GuardrailWindowAggregate> harness =
                     new KeyedOneInputStreamOperatorTestHarness<>(
                             operator,
                             new GuardrailAggregateKeySelector(),
                             TypeInformation.of(new TypeHint<>() {
                             })
                     )) {
            harness.open();
            harness.processElement(buildFinding("req-000010", BASE_TIME + 5_000L, null, true, 10L, GuardrailNames.LOOPING), BASE_TIME + 5_000L);
            harness.processWatermark(FIRST_WINDOW_END);

            GuardrailWindowAggregate aggregate = (GuardrailWindowAggregate) harness.extractOutputStreamRecords().get(0).getValue();
            assertNull(aggregate.p50Confidence());
            assertNull(aggregate.p95Confidence());
            assertNull(aggregate.triggeredP50Confidence());
            assertNull(aggregate.triggeredP95Confidence());
        }
    }

    private static WindowOperator<GuardrailAggregateKey, SafetyEvent, ?, GuardrailWindowAggregate, TimeWindow> buildWindowOperator(
            Time allowedLateness
    ) {
        WindowOperatorBuilder<SafetyEvent, GuardrailAggregateKey, TimeWindow> builder =
                new WindowOperatorBuilder<>(
                        TumblingEventTimeWindows.of(Time.minutes(1)),
                        EventTimeTrigger.create(),
                        new ExecutionConfig(),
                        TypeInformation.of(SafetyEvent.class),
                        new GuardrailAggregateKeySelector(),
                        TypeInformation.of(new TypeHint<>() {
                        })
                );
        builder.allowedLateness(allowedLateness);
        return builder.aggregate(
                new GuardrailWindowAggregateFunction(),
                new GuardrailWindowProcessFunction("1m"),
                TypeInformation.of(GuardrailAggregateAccumulator.class)
        );
    }

    private static SafetyEvent buildFinding(
            String requestId,
            long eventTime,
            Double confidence,
            boolean triggered,
            Long detectorLatencyMs
    ) {
        return buildFinding(requestId, eventTime, confidence, triggered, detectorLatencyMs, GuardrailNames.PROMPT_INJECTION);
    }

    private static SafetyEvent buildFinding(
            String requestId,
            long eventTime,
            Double confidence,
            boolean triggered,
            Long detectorLatencyMs,
            String guardrailName
    ) {
        return new SafetyEvent(
                TENANT_ID,
                null,
                AGENT_ID,
                SESSION_ID,
                requestId,
                "turn-" + requestId,
                eventTime,
                EventType.GUARDRAIL_FINDING,
                MODEL_NAME,
                "user-001",
                "web",
                100,
                50,
                guardrailName,
                GUARDRAIL_VERSION,
                POLICY_VERSION,
                confidence,
                triggered,
                detectorLatencyMs,
                "OK",
                "{}"
        );
    }
}
