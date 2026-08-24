package com.yourjavaguy.pitwall.serving.query;

public enum Resolution {

    ONE_MINUTE("telemetry_rollup_1m"),
    ONE_HOUR("telemetry_rollup_1h");

    private final String viewName;

    Resolution(String viewName) {
        this.viewName = viewName;
    }

    public String viewName() {
        return viewName;
    }
}
