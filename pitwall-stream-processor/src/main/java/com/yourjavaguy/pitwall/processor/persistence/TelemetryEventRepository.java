package com.yourjavaguy.pitwall.processor.persistence;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

@Repository
public class TelemetryEventRepository {

    private static final String INSERT = """
            insert into telemetry_event (car_id, sensor_id, event_time, value, sequence_no)
            values (?, ?, ?, ?, ?)
            on conflict (car_id, sensor_id, event_time) do nothing
            """;

    private final JdbcTemplate jdbcTemplate;

    public TelemetryEventRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public int insertBatch(List<TelemetryEvent> events) {
        var rowCounts = jdbcTemplate.batchUpdate(INSERT, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement statement, int index) throws SQLException {
                var event = events.get(index);
                statement.setString(1, event.carId());
                statement.setString(2, event.sensorId());
                statement.setTimestamp(3, Timestamp.from(event.timestamp()));
                statement.setDouble(4, event.value());
                statement.setLong(5, event.sequenceNo());
            }

            @Override
            public int getBatchSize() {
                return events.size();
            }
        });
        return countInserted(rowCounts, events.size());
    }

    private static int countInserted(int[] rowCounts, int attempted) {
        var inserted = 0;
        for (var rowCount : rowCounts) {
            if (rowCount == Statement.SUCCESS_NO_INFO) {
                return attempted;
            }
            inserted += rowCount;
        }
        return inserted;
    }

    public long count() {
        return jdbcTemplate.queryForObject("select count(*) from telemetry_event", Long.class);
    }

    public long countBySensor(String sensorId) {
        return jdbcTemplate.queryForObject(
                "select count(*) from telemetry_event where sensor_id = ?", Long.class, sensorId);
    }

    public long countDistinctReadings() {
        return jdbcTemplate.queryForObject(
                "select count(*) from (select distinct car_id, sensor_id, event_time from telemetry_event) as readings",
                Long.class);
    }
}
