package com.yourjavaguy.pitwall.processor.config;

import com.yourjavaguy.pitwall.schema.WireFormat;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "pitwall.processor")
public record ProcessorProperties(
        @DefaultValue("telemetry.events") String topic,
        @DefaultValue("500") int writeBatchSize,
        @DefaultValue("PROTOBUF") WireFormat wireFormat,
        @DefaultValue Rollup rollup,
        @DefaultValue List<AlertRule> alerts
) {

    public record Rollup(
            @DefaultValue("telemetry.rollups") String topic,
            @DefaultValue("telemetry.alerts") String alertTopic,
            @DefaultValue({"1s", "1m"}) List<Duration> windows,
            @DefaultValue("10s") Duration grace
    ) {
    }

    public record AlertRule(String sensorId, double maximum) {
    }
}
