package com.yourjavaguy.pitwall.serving.query;

import com.yourjavaguy.pitwall.commons.exception.ResourceNotFoundException;
import com.yourjavaguy.pitwall.serving.config.ServingProperties;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;

@Service
public class TelemetryQueryService {

    private final TelemetryRollupRepository repository;
    private final int maximumQueryRows;

    public TelemetryQueryService(TelemetryRollupRepository repository, ServingProperties properties) {
        this.repository = repository;
        this.maximumQueryRows = properties.maximumQueryRows();
    }

    public List<RollupRow> rollups(Resolution resolution,
                                   String carId,
                                   String sensorId,
                                   Instant from,
                                   Instant to) {
        var rows = repository.findRollups(resolution, carId, sensorId, from, to, maximumQueryRows);
        if (rows.isEmpty()) {
            throw new ResourceNotFoundException("Rollups", "%s/%s".formatted(carId, sensorId));
        }
        return rows;
    }

    public List<String> carIds() {
        return repository.findCarIds();
    }

    public List<String> sensorIds() {
        return repository.findSensorIds();
    }
}
