package com.yourjavaguy.pitwall.source.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import com.yourjavaguy.pitwall.schema.WireFormat;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "pitwall.source")
public record SourceProperties(
        @DefaultValue("20") int cars,
        @DefaultValue("100") int sensorsPerCar,
        @DefaultValue("1.0") double rateScale,
        @DefaultValue("90") int lapDurationSeconds,
        @DefaultValue("true") boolean autoStart,
        @DefaultValue("20000") int logSampleInterval,
        @DefaultValue("kafka") String sink,
        @DefaultValue("PROTOBUF") WireFormat wireFormat,
        @DefaultValue Dropout dropout,
        @DefaultValue Kafka kafka
) {

    public record Dropout(
            @DefaultValue("500000") int bufferCapacity,
            @DefaultValue("2000") int overlapSize,
            @DefaultValue("5000") int replayBatchSize
    ) {
    }

    public record Kafka(
            @DefaultValue("telemetry.events") String topic,
            @DefaultValue("12") int partitions,
            @DefaultValue("1") short replicationFactor
    ) {
    }
}
