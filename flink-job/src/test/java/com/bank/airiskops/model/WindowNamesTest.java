package com.bank.airiskops.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WindowNamesTest {
    @Test
    void mapsKnownAndCustomDurationsToStableNames() {
        assertEquals("1m", WindowNames.forDuration(Duration.ofMinutes(1)));
        assertEquals("5m", WindowNames.forDuration(Duration.ofMinutes(5)));
        assertEquals("15m", WindowNames.forDuration(Duration.ofMinutes(15)));
        assertEquals("30s", WindowNames.forDuration(Duration.ofSeconds(30)));
    }
}
