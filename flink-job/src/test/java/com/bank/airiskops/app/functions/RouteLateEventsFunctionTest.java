package com.bank.airiskops.app.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.bank.airiskops.app.support.JobTopology;
import com.bank.airiskops.model.EventType;
import com.bank.airiskops.model.LateEvent;
import com.bank.airiskops.model.SafetyEvent;
import java.time.Duration;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.jupiter.api.Test;

class RouteLateEventsFunctionTest {
    private static final Duration LATE_TOLERANCE = Duration.ofMinutes(5);
    private static final long CURRENT_WATERMARK = 600_000L;
    private static final String AGENT_ID = "agent-risk-01";
    private static final String SESSION_ID = "session-001";
    private static final String REQUEST_ID = "req-000001";
    private static final String MODEL_NAME = "gpt-4.1-mini";
    private static final String USER_ID = "user-1";
    private static final String CHANNEL = "web";
    private static final int INPUT_TOKENS = 10;
    private static final long EVENT_TIME = 0L;
    private static final String RAW_PAYLOAD = "{}";

    @Test
    void routesLateEventsToSideOutput() throws Exception {
        RouteLateEventsFunction function = new RouteLateEventsFunction(LATE_TOLERANCE, JobTopology.LATE_EVENTS);
        try (OneInputStreamOperatorTestHarness<SafetyEvent, SafetyEvent> harness =
                     ProcessFunctionTestHarnesses.forProcessFunction(function)) {
            harness.open();
            // Watermark at 10 minutes with 5-minute tolerance means an event at 0
            // must be classified as late and diverted into the side output.
            harness.processWatermark(CURRENT_WATERMARK);
            SafetyEvent lateEvent = new SafetyEvent(
                    AGENT_ID,
                    null,
                    AGENT_ID,
                    SESSION_ID,
                    REQUEST_ID,
                    null,
                    EVENT_TIME,
                    EventType.AGENT_REQUEST,
                    MODEL_NAME,
                    USER_ID,
                    CHANNEL,
                    INPUT_TOKENS,
                    0,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    RAW_PAYLOAD
            );

            harness.processElement(lateEvent, lateEvent.eventTimeMillis());

            assertTrue(harness.extractOutputValues().isEmpty());
            LateEvent sideOutput = (LateEvent) harness.getSideOutput(JobTopology.LATE_EVENTS).poll().getValue();
            assertEquals(REQUEST_ID, sideOutput.event().requestId());
        }
    }
}
