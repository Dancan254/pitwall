package com.yourjavaguy.pitwall.source.web;

import com.yourjavaguy.pitwall.source.control.SourceControlService;
import com.yourjavaguy.pitwall.source.control.SourceStatus;
import com.yourjavaguy.pitwall.source.web.dto.AnomalyRequest;
import com.yourjavaguy.pitwall.source.web.dto.BurstRequest;
import com.yourjavaguy.pitwall.source.web.dto.DropoutRequest;
import com.yourjavaguy.pitwall.source.web.dto.LoadRequest;
import com.yourjavaguy.pitwall.source.web.dto.ReplayResponse;
import com.yourjavaguy.pitwall.source.web.dto.SinkRequest;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/source")
public class SourceController {

    private final SourceControlService controlService;

    public SourceController(SourceControlService controlService) {
        this.controlService = controlService;
    }

    @GetMapping("/status")
    public ResponseEntity<SourceStatus> status() {
        return ResponseEntity.ok(controlService.status());
    }

    @PostMapping("/start")
    public ResponseEntity<SourceStatus> start() {
        return ResponseEntity.ok(controlService.start());
    }

    @PostMapping("/stop")
    public ResponseEntity<SourceStatus> stop() {
        return ResponseEntity.ok(controlService.stop());
    }

    @PutMapping("/load")
    public ResponseEntity<SourceStatus> applyLoad(@Valid @RequestBody LoadRequest request) {
        return ResponseEntity.ok(
                controlService.applyLoad(request.cars(), request.sensorsPerCar(), request.rateScale()));
    }

    @PutMapping("/sink")
    public ResponseEntity<SourceStatus> useSink(@Valid @RequestBody SinkRequest request) {
        return ResponseEntity.ok(controlService.useSink(request.name()));
    }

    @PutMapping("/faults/dropout")
    public ResponseEntity<SourceStatus> setDropout(@Valid @RequestBody DropoutRequest request) {
        return ResponseEntity.ok(controlService.setDropout(request.active()));
    }

    @PostMapping("/faults/replay")
    public ResponseEntity<ReplayResponse> replay() {
        return ResponseEntity.accepted().body(new ReplayResponse(controlService.triggerReplay()));
    }

    @PostMapping("/faults/burst")
    public ResponseEntity<SourceStatus> startBurst(@Valid @RequestBody BurstRequest request) {
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(controlService.startBurst(request.multiplier(), request.durationSeconds()));
    }

    @PutMapping("/faults/anomaly")
    public ResponseEntity<SourceStatus> setAnomaly(@Valid @RequestBody AnomalyRequest request) {
        return ResponseEntity.ok(controlService.setAnomaly(request.carId(), request.sensorId()));
    }

    @DeleteMapping("/faults/anomaly")
    public ResponseEntity<SourceStatus> clearAnomaly() {
        return ResponseEntity.ok(controlService.clearAnomaly());
    }
}
