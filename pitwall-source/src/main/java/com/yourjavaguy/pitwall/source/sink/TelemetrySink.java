package com.yourjavaguy.pitwall.source.sink;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;

public interface TelemetrySink {

    void publish(TelemetryEvent event);

    String name();
}
