package com.yourjavaguy.pitwall.schema;

import com.google.protobuf.InvalidProtocolBufferException;
import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.schema.proto.TelemetryEventMessage;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class TelemetryEventProtobufDeserializer implements Deserializer<TelemetryEvent> {

    @Override
    public TelemetryEvent deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return TelemetryEventWireFormat.toEvent(TelemetryEventMessage.parseFrom(bytes));
        } catch (InvalidProtocolBufferException exception) {
            throw new SerializationException("Not a valid TelemetryEventMessage on topic " + topic, exception);
        }
    }
}
