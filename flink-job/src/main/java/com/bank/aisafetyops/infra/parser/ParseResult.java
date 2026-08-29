package com.bank.aisafetyops.infra.parser;

import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.SafetyEvent;

/**
 * Result of parsing one raw input payload.
 *
 * <p>Exactly one branch is expected to be populated: either a valid
 * {@code SafetyEvent} or an {@code InvalidEvent} that explains why the payload
 * cannot safely continue through the main pipeline.
 */
public record ParseResult(
        SafetyEvent event,
        InvalidEvent invalidEvent
) {
    public static ParseResult success(SafetyEvent event) {
        return new ParseResult(event, null);
    }

    public static ParseResult invalid(String reason, String rawPayload) {
        return new ParseResult(null, new InvalidEvent(reason, rawPayload));
    }

    public boolean isValid() {
        return event != null;
    }
}
