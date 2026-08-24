package com.yourjavaguy.pitwall.serving.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties(prefix = "pitwall.serving")
public record ServingProperties(
        @DefaultValue("telemetry.rollups") String rollupTopic,
        @DefaultValue("telemetry.alerts") String alertTopic,
        @DefaultValue("30m") Duration streamTimeout,
        @DefaultValue("200") int alertHistorySize,
        @DefaultValue("5000") int maximumQueryRows
) {
}
