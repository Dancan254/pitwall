package com.yourjavaguy.pitwall.processor.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;

@Component
public class IngestMetrics {

    private final Counter consumedEvents;
    private final Counter writtenEvents;
    private final Counter duplicateEvents;
    private final DistributionSummary pollBatchSize;
    private final Timer writeDuration;
    private final AtomicLong consumedEventsPerSecond = new AtomicLong();

    private long lastConsumedCount;

    public IngestMetrics(MeterRegistry meterRegistry) {
        this.consumedEvents = Counter.builder("pitwall.processor.events.consumed")
                .description("Telemetry events polled from Kafka")
                .register(meterRegistry);
        this.writtenEvents = Counter.builder("pitwall.processor.events.written")
                .description("Telemetry events inserted into the store")
                .register(meterRegistry);
        this.duplicateEvents = Counter.builder("pitwall.processor.events.duplicates")
                .description("Telemetry events the natural key rejected as already stored")
                .register(meterRegistry);
        this.pollBatchSize = DistributionSummary.builder("pitwall.processor.poll.batch.size")
                .description("Events returned by a single Kafka poll")
                .register(meterRegistry);
        this.writeDuration = Timer.builder("pitwall.processor.write.duration")
                .description("Time to write one batch to the store")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);

        Gauge.builder("pitwall.processor.rate.consumed", consumedEventsPerSecond, AtomicLong::doubleValue)
                .description("Events consumed per second, sampled every second")
                .register(meterRegistry);
    }

    public void recordPoll(int batchSize) {
        consumedEvents.increment(batchSize);
        pollBatchSize.record(batchSize);
    }

    public void recordWrite(int attempted, IntSupplier write) {
        var inserted = writeDuration.record(write::getAsInt);
        writtenEvents.increment(inserted);
        duplicateEvents.increment(attempted - inserted);
    }

    public long consumedCount() {
        return (long) consumedEvents.count();
    }

    public long writtenCount() {
        return (long) writtenEvents.count();
    }

    public long duplicateCount() {
        return (long) duplicateEvents.count();
    }

    @Scheduled(fixedRate = 1000)
    void sampleConsumeRate() {
        var current = consumedCount();
        consumedEventsPerSecond.set(current - lastConsumedCount);
        lastConsumedCount = current;
    }
}
