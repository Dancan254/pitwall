package com.yourjavaguy.pitwall.source.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class SourceMetrics {

    private final Counter publishedEvents;
    private final Counter bufferedEvents;
    private final Counter droppedEvents;
    private final Counter replayedEvents;
    private final Counter failedEvents;

    private final AtomicLong targetEventsPerSecond = new AtomicLong();
    private final AtomicLong actualEventsPerSecond = new AtomicLong();
    private final AtomicLong bufferDepth = new AtomicLong();

    private long lastPublishedCount;

    public SourceMetrics(MeterRegistry meterRegistry) {
        this.publishedEvents = Counter.builder("pitwall.source.events.published")
                .description("Telemetry events handed to the sink")
                .register(meterRegistry);
        this.bufferedEvents = Counter.builder("pitwall.source.events.buffered")
                .description("Telemetry events held back during a dropout")
                .register(meterRegistry);
        this.droppedEvents = Counter.builder("pitwall.source.events.dropped")
                .description("Telemetry events lost because the dropout buffer was full")
                .register(meterRegistry);
        this.replayedEvents = Counter.builder("pitwall.source.events.replayed")
                .description("Telemetry events re-sent after a dropout, duplicates included")
                .register(meterRegistry);

        this.failedEvents = Counter.builder("pitwall.source.events.failed")
                .description("Telemetry events the active sink rejected")
                .register(meterRegistry);

        Gauge.builder("pitwall.source.rate.target", targetEventsPerSecond, AtomicLong::doubleValue)
                .description("Events per second the load dial is configured to produce")
                .register(meterRegistry);
        Gauge.builder("pitwall.source.rate.actual", actualEventsPerSecond, AtomicLong::doubleValue)
                .description("Events per second actually published, sampled every second")
                .register(meterRegistry);
        Gauge.builder("pitwall.source.buffer.depth", bufferDepth, AtomicLong::doubleValue)
                .description("Events currently held in the dropout buffer")
                .register(meterRegistry);
    }

    public void recordPublished() {
        publishedEvents.increment();
    }

    public void recordBuffered() {
        bufferedEvents.increment();
    }

    public void recordDropped() {
        droppedEvents.increment();
    }

    public void recordReplayed(long count) {
        replayedEvents.increment(count);
    }

    public void recordFailed() {
        failedEvents.increment();
    }

    public long failedCount() {
        return (long) failedEvents.count();
    }

    public void setTargetEventsPerSecond(long target) {
        targetEventsPerSecond.set(target);
    }

    public void setBufferDepth(long depth) {
        bufferDepth.set(depth);
    }

    public long targetEventsPerSecond() {
        return targetEventsPerSecond.get();
    }

    public long actualEventsPerSecond() {
        return actualEventsPerSecond.get();
    }

    public long publishedCount() {
        return (long) publishedEvents.count();
    }

    @Scheduled(fixedRate = 1000)
    void samplePublishRate() {
        var current = publishedCount();
        actualEventsPerSecond.set(current - lastPublishedCount);
        lastPublishedCount = current;
    }
}
