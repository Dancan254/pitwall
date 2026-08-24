package com.yourjavaguy.pitwall.schema;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.apache.kafka.common.serialization.Serializer;

public class TelemetryEventProtobufSerializer implements Serializer<TelemetryEvent> {

    @Override
    public byte[] serialize(String topic, TelemetryEvent event) {
        if (event == null) {
            return null;
        }
        return TelemetryEventWireFormat.toMessage(event).toByteArray();
    }
}
