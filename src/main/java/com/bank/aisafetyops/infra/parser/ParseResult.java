package com.bank.aisafetyops.infra.parser;

import com.bank.aisafetyops.model.InvalidEvent;
import com.bank.aisafetyops.model.SafetyEvent;

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
