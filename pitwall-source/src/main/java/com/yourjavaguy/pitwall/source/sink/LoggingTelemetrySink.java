package com.yourjavaguy.pitwall.source.sink;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class LoggingTelemetrySink implements TelemetrySink {

    private static final Logger log = LoggerFactory.getLogger(LoggingTelemetrySink.class);

    private final int sampleInterval;
    private final AtomicLong seen = new AtomicLong();

    public LoggingTelemetrySink(SourceProperties properties) {
        this.sampleInterval = Math.max(1, properties.logSampleInterval());
    }

    @Override
    public void publish(TelemetryEvent event) {
        if (seen.incrementAndGet() % sampleInterval == 0) {
            log.info("sample car={} sensor={} value={} sequence={} at={}",
                    event.carId(), event.sensorId(), event.value(), event.sequenceNo(), event.timestamp());
        }
    }

    @Override
    public String name() {
        return "logging";
    }
}
