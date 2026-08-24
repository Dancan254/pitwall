package com.yourjavaguy.pitwall.processor.rollup;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class TelemetryEventTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof TelemetryEvent event && event.timestamp() != null) {
            return event.timestamp().toEpochMilli();
        }
        return partitionTime;
    }
}
