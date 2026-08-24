package com.yourjavaguy.pitwall.source.control;

import com.yourjavaguy.pitwall.source.fault.FaultInjection;
import com.yourjavaguy.pitwall.source.generator.LoadDial;
import com.yourjavaguy.pitwall.source.generator.TelemetryGenerator;
import com.yourjavaguy.pitwall.source.metrics.SourceMetrics;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class SourceControlService {

    private static final Logger log = LoggerFactory.getLogger(SourceControlService.class);

    private final TelemetryGenerator generator;
    private final LoadDial loadDial;
    private final FaultInjection faults;
    private final SourceMetrics metrics;

    public SourceControlService(TelemetryGenerator generator,
                                LoadDial loadDial,
                                FaultInjection faults,
                                SourceMetrics metrics) {
        this.generator = generator;
        this.loadDial = loadDial;
        this.faults = faults;
        this.metrics = metrics;
    }

    @PostConstruct
    void publishInitialTargetRate() {
        metrics.setTargetEventsPerSecond(loadDial.targetEventsPerSecond());
    }

    public SourceStatus status() {
        return new SourceStatus(
                generator.isRunning(),
                generator.activeSinkName(),
                generator.availableSinkNames(),
                loadDial.cars(),
                loadDial.sensorsPerCar(),
                loadDial.rateScale(),
                loadDial.targetEventsPerSecond(),
                metrics.actualEventsPerSecond(),
                metrics.publishedCount(),
                metrics.failedCount(),
                faults.isDropoutActive(),
                faults.bufferDepth(),
                faults.burstMultiplier(),
                faults.anomalyCarId(),
                faults.anomalySensorId());
    }

    public SourceStatus start() {
        generator.start();
        return status();
    }

    public SourceStatus stop() {
        generator.stop();
        return status();
    }

    public SourceStatus applyLoad(int cars, int sensorsPerCar, double rateScale) {
        loadDial.apply(cars, sensorsPerCar, rateScale);
        metrics.setTargetEventsPerSecond(loadDial.targetEventsPerSecond());
        generator.restart();
        return status();
    }

    public SourceStatus setDropout(boolean active) {
        if (active) {
            faults.startDropout();
            log.info("dropout started, telemetry is now buffering");
            return status();
        }
        faults.stopDropout();
        log.info("dropout ended, {} events buffered and awaiting replay", faults.bufferDepth());
        return status();
    }

    public long triggerReplay() {
        var pending = faults.bufferDepth();
        faults.stopDropout();
        Thread.ofVirtual().name("telemetry-replay").start(() -> {
            var replayed = generator.replayBuffered();
            log.info("replay finished, {} events re-sent including the duplicate overlap", replayed);
        });
        return pending;
    }

    public SourceStatus startBurst(double multiplier, int durationSeconds) {
        faults.setBurstMultiplier(multiplier);
        log.info("burst started multiplier={} durationSeconds={}", multiplier, durationSeconds);
        Thread.ofVirtual().name("telemetry-burst").start(() -> endBurstAfter(durationSeconds));
        return status();
    }

    public SourceStatus useSink(String name) {
        generator.useSink(name);
        return status();
    }

    public SourceStatus setAnomaly(String carId, String sensorId) {
        faults.startAnomaly(carId, sensorId);
        return status();
    }

    public SourceStatus clearAnomaly() {
        faults.stopAnomaly();
        return status();
    }

    private void endBurstAfter(int durationSeconds) {
        try {
            Thread.sleep(Duration.ofSeconds(durationSeconds));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return;
        }
        faults.setBurstMultiplier(1.0);
        log.info("burst ended");
    }
}
