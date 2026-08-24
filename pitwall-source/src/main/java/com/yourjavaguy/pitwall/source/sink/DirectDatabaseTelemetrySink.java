package com.yourjavaguy.pitwall.source.sink;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

@Component
public class DirectDatabaseTelemetrySink implements TelemetrySink {

    private static final String INSERT = """
            insert into telemetry_event (car_id, sensor_id, event_time, value, sequence_no)
            values (?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public DirectDatabaseTelemetrySink(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void publish(TelemetryEvent event) {
        jdbcTemplate.update(INSERT,
                event.carId(),
                event.sensorId(),
                Timestamp.from(event.timestamp()),
                event.value(),
                event.sequenceNo());
    }

    @Override
    public String name() {
        return "database";
    }
}
