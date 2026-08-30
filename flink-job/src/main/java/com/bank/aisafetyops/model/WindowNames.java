package com.bank.aisafetyops.model;

import java.time.Duration;

/**
 * Stable names for business aggregation windows exposed to Kafka and dashboards.
 */
public final class WindowNames {
    public static final String WINDOW_1_MINUTE = "1m";
    public static final String WINDOW_5_MINUTES = "5m";

    private WindowNames() {
    }

    public static String forDuration(Duration duration) {
        long minutes = duration.toMinutes();
        if (duration.equals(Duration.ofMinutes(1))) {
            return WINDOW_1_MINUTE;
        }
        if (duration.equals(Duration.ofMinutes(5))) {
            return WINDOW_5_MINUTES;
        }
        if (minutes > 0 && duration.equals(Duration.ofMinutes(minutes))) {
            return minutes + "m";
        }
        long seconds = duration.toSeconds();
        return seconds + "s";
    }
}
