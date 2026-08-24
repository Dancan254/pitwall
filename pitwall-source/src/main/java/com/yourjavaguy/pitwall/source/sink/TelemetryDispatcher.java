package com.yourjavaguy.pitwall.source.sink;

import com.yourjavaguy.pitwall.commons.event.TelemetryEvent;
import com.yourjavaguy.pitwall.source.config.SourceProperties;
import com.yourjavaguy.pitwall.source.fault.FaultInjection;
import com.yourjavaguy.pitwall.source.metrics.SourceMetrics;
import com.yourjavaguy.pitwall.commons.exception.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class TelemetryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(TelemetryDispatcher.class);

    private final Map<String, TelemetrySink> sinksByName;
    private final FaultInjection faults;
    private final SourceMetrics metrics;

    private volatile TelemetrySink activeSink;

    public TelemetryDispatcher(List<TelemetrySink> sinks,
                               SourceProperties properties,
                               FaultInjection faults,
                               SourceMetrics metrics) {
        this.sinksByName = sinks.stream().collect(Collectors.toMap(TelemetrySink::name, Function.identity()));
        this.faults = faults;
        this.metrics = metrics;
        this.activeSink = requireSink(properties.sink());
        log.info("dispatching telemetry to the {} sink, available sinks are {}",
                activeSink.name(), sinksByName.keySet());
    }

    public void dispatch(TelemetryEvent event) {
        if (faults.isDropoutActive()) {
            if (faults.buffer(event)) {
                metrics.recordBuffered();
                return;
            }
            metrics.recordDropped();
            return;
        }
        deliver(event);
    }

    public long replayBuffered() {
        var overlap = faults.takeOverlap();
        var replayed = (long) overlap.size();
        overlap.forEach(this::deliver);
        metrics.recordReplayed(overlap.size());

        while (faults.hasBufferedEvents()) {
            var batch = faults.nextBufferedBatch();
            batch.forEach(this::deliver);
            metrics.recordReplayed(batch.size());
            replayed += batch.size();
        }
        return replayed;
    }

    public String activeSinkName() {
        return activeSink.name();
    }

    public List<String> availableSinkNames() {
        return sinksByName.keySet().stream().sorted().toList();
    }

    public void useSink(String name) {
        activeSink = requireSink(name);
        log.info("dispatching telemetry to the {} sink", name);
    }

    private void deliver(TelemetryEvent event) {
        try {
            activeSink.publish(event);
        } catch (RuntimeException exception) {
            metrics.recordFailed();
            return;
        }
        faults.recordDelivered(event);
        metrics.recordPublished();
    }

    private TelemetrySink requireSink(String name) {
        var sink = sinksByName.get(name);
        if (sink == null) {
            throw new ResourceNotFoundException("Telemetry sink", name);
        }
        return sink;
    }

    @Scheduled(fixedRate = 1000)
    void sampleBufferDepth() {
        metrics.setBufferDepth(faults.bufferDepth());
    }
}
