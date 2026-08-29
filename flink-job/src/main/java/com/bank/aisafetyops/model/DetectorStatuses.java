package com.bank.aisafetyops.model;

/**
 * Shared detector status constants and helpers.
 *
 * <p>The current MVP treats missing status and explicit {@code OK} as healthy
 * detector outcomes when computing aggregate error counters.
 */
public final class DetectorStatuses {
    public static final String OK = "OK";

    private DetectorStatuses() {
    }

    public static boolean isOk(String detectorStatus) {
        return detectorStatus == null || OK.equalsIgnoreCase(detectorStatus);
    }
}
