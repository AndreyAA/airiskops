package com.bank.aisafetyops.app.support;

import java.time.Duration;

public final class FlinkEnvironmentDefaults {
    public static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(30);
    public static final Duration AUTO_WATERMARK_INTERVAL = Duration.ofSeconds(5);

    private FlinkEnvironmentDefaults() {
    }
}
