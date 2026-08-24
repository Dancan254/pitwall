package com.yourjavaguy.pitwall.schema;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class TelemetryEventProtobufSerde implements Serde<TelemetryEvent> {

    private final TelemetryEventProtobufSerializer serializer = new TelemetryEventProtobufSerializer();
    private final TelemetryEventProtobufDeserializer deserializer = new TelemetryEventProtobufDeserializer();

    @Override
    public Serializer<TelemetryEvent> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<TelemetryEvent> deserializer() {
        return deserializer;
    }
}
