package com.yourjavaguy.pitwall.source.fault;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

@Component
public class FaultInjection {

    private final int bufferCapacity;
    private final int overlapSize;
    private final int replayBatchSize;

    private final Deque<TelemetryEvent> dropoutBuffer;
    private final Deque<TelemetryEvent> recentlyDelivered;

    private volatile boolean dropoutActive;
    private volatile double burstMultiplier = 1.0;
    private volatile String anomalyCarId;
    private volatile String anomalySensorId;

    public FaultInjection(SourceProperties properties) {
        this.bufferCapacity = properties.dropout().bufferCapacity();
        this.overlapSize = properties.dropout().overlapSize();
        this.replayBatchSize = properties.dropout().replayBatchSize();
        this.dropoutBuffer = new ArrayDeque<>();
        this.recentlyDelivered = new ArrayDeque<>();
    }

    public boolean isDropoutActive() {
        return dropoutActive;
    }

    public void startDropout() {
        dropoutActive = true;
    }

    public void stopDropout() {
        dropoutActive = false;
    }

    public boolean buffer(TelemetryEvent event) {
        synchronized (dropoutBuffer) {
            if (dropoutBuffer.size() >= bufferCapacity) {
                return false;
            }
            dropoutBuffer.addLast(event);
            return true;
        }
    }

    public void recordDelivered(TelemetryEvent event) {
        if (overlapSize <= 0) {
            return;
        }
        synchronized (recentlyDelivered) {
            if (recentlyDelivered.size() >= overlapSize) {
                recentlyDelivered.removeFirst();
            }
            recentlyDelivered.addLast(event);
        }
    }

    public List<TelemetryEvent> takeOverlap() {
        synchronized (recentlyDelivered) {
            var overlap = List.copyOf(recentlyDelivered);
            recentlyDelivered.clear();
            return overlap;
        }
    }

    public List<TelemetryEvent> nextBufferedBatch() {
        var batch = new ArrayList<TelemetryEvent>(replayBatchSize);
        synchronized (dropoutBuffer) {
            while (batch.size() < replayBatchSize && !dropoutBuffer.isEmpty()) {
                batch.add(dropoutBuffer.removeFirst());
            }
        }
        return batch;
    }

    public boolean hasBufferedEvents() {
        synchronized (dropoutBuffer) {
            return !dropoutBuffer.isEmpty();
        }
    }

    public long bufferDepth() {
        synchronized (dropoutBuffer) {
            return dropoutBuffer.size();
        }
    }

    public double burstMultiplier() {
        return burstMultiplier;
    }

    public void setBurstMultiplier(double multiplier) {
        this.burstMultiplier = multiplier;
    }

    public boolean isAnomalous(String carId, String sensorId) {
        return Objects.equals(anomalyCarId, carId) && Objects.equals(anomalySensorId, sensorId);
    }

    public void startAnomaly(String carId, String sensorId) {
        this.anomalyCarId = carId;
        this.anomalySensorId = sensorId;
    }

    public void stopAnomaly() {
        this.anomalyCarId = null;
        this.anomalySensorId = null;
    }

    public String anomalyCarId() {
        return anomalyCarId;
    }

    public String anomalySensorId() {
        return anomalySensorId;
    }
}
