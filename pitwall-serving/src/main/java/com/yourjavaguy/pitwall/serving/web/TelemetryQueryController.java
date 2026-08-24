package com.yourjavaguy.pitwall.serving.web;

import com.yourjavaguy.pitwall.serving.query.Resolution;
import com.yourjavaguy.pitwall.serving.query.RollupRow;
import com.yourjavaguy.pitwall.serving.query.TelemetryQueryService;
import jakarta.validation.constraints.NotBlank;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/api/v1/telemetry")
@Validated
public class TelemetryQueryController {

    private final TelemetryQueryService queryService;

    public TelemetryQueryController(TelemetryQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/cars")
    public ResponseEntity<List<String>> cars() {
        return ResponseEntity.ok(queryService.carIds());
    }

    @GetMapping("/sensors")
    public ResponseEntity<List<String>> sensors() {
        return ResponseEntity.ok(queryService.sensorIds());
    }

    @GetMapping("/rollups")
    public ResponseEntity<List<RollupRow>> rollups(
            @RequestParam @NotBlank String carId,
            @RequestParam @NotBlank String sensorId,
            @RequestParam(defaultValue = "ONE_MINUTE") Resolution resolution,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {

        var end = to != null ? to : Instant.now();
        var start = from != null ? from : end.minus(Duration.ofHours(1));
        return ResponseEntity.ok(queryService.rollups(resolution, carId, sensorId, start, end));
    }
}
