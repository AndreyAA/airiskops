package com.bank.aisafetyops.model;

public final class DetectorStatuses {
    public static final String OK = "OK";

    private DetectorStatuses() {
    }

    public static boolean isOk(String detectorStatus) {
        return detectorStatus == null || OK.equalsIgnoreCase(detectorStatus);
    }
}
