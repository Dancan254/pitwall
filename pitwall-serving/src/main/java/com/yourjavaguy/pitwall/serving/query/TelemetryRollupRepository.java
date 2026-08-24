package com.yourjavaguy.pitwall.serving.query;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

@Repository
public class TelemetryRollupRepository {

    private static final String SELECT_ROLLUPS = """
            select car_id, sensor_id, bucket, samples, average, minimum, maximum
            from %s
            where car_id = ? and sensor_id = ? and bucket >= ? and bucket < ?
            order by bucket
            limit ?
            """;

    private final JdbcTemplate jdbcTemplate;

    public TelemetryRollupRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<RollupRow> findRollups(Resolution resolution,
                                       String carId,
                                       String sensorId,
                                       Instant from,
                                       Instant to,
                                       int limit) {
        return jdbcTemplate.query(
                SELECT_ROLLUPS.formatted(resolution.viewName()),
                (resultSet, rowNumber) -> new RollupRow(
                        resultSet.getString("car_id"),
                        resultSet.getString("sensor_id"),
                        resultSet.getTimestamp("bucket").toInstant(),
                        resultSet.getLong("samples"),
                        resultSet.getDouble("average"),
                        resultSet.getDouble("minimum"),
                        resultSet.getDouble("maximum")),
                carId, sensorId, Timestamp.from(from), Timestamp.from(to), limit);
    }

    public List<String> findCarIds() {
        return jdbcTemplate.queryForList(
                "select distinct car_id from telemetry_rollup_1m order by car_id", String.class);
    }

    public List<String> findSensorIds() {
        return jdbcTemplate.queryForList(
                "select distinct sensor_id from telemetry_rollup_1m order by sensor_id", String.class);
    }
}
