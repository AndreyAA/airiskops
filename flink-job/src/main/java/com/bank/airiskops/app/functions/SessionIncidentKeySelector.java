package com.bank.airiskops.app.functions;

import com.bank.airiskops.model.SafetyEvent;
import com.bank.airiskops.model.SessionIncidentKey;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Selects the agreed session correlation key for incident generation.
 */
public final class SessionIncidentKeySelector implements KeySelector<SafetyEvent, SessionIncidentKey> {
    @Override
    public SessionIncidentKey getKey(SafetyEvent event) {
        return new SessionIncidentKey(event.agentId(), event.sessionId());
    }
}
