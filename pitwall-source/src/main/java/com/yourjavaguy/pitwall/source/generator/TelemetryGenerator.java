package com.yourjavaguy.pitwall.source.generator;

import com.yourjavaguy.pitwall.source.config.SourceProperties;
import com.yourjavaguy.pitwall.source.sensor.SensorCatalog;
import com.yourjavaguy.pitwall.source.sink.TelemetryDispatcher;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class TelemetryGenerator {

    private static final Logger log = LoggerFactory.getLogger(TelemetryGenerator.class);

    private final SourceProperties properties;
    private final SensorCatalog sensorCatalog;
    private final TelemetryDispatcher dispatcher;
    private final LoadDial loadDial;

    private final List<CarEmitter> emitters = new ArrayList<>();
    private final List<Thread> emitterThreads = new ArrayList<>();

    private volatile boolean running;

    public TelemetryGenerator(SourceProperties properties,
                              SensorCatalog sensorCatalog,
                              TelemetryDispatcher dispatcher,
                              LoadDial loadDial) {
        this.properties = properties;
        this.sensorCatalog = sensorCatalog;
        this.dispatcher = dispatcher;
        this.loadDial = loadDial;
    }

    @EventListener(ApplicationReadyEvent.class)
    void startIfConfigured() {
        if (properties.autoStart()) {
            start();
        }
    }

    @PreDestroy
    public synchronized void stop() {
        if (!running) {
            return;
        }
        emitters.forEach(CarEmitter::stop);
        emitterThreads.forEach(Thread::interrupt);
        emitterThreads.forEach(this::joinQuietly);
        emitters.clear();
        emitterThreads.clear();
        running = false;
        log.info("telemetry generator stopped");
    }

    public synchronized void start() {
        if (running) {
            return;
        }
        for (var carIndex = 1; carIndex <= loadDial.cars(); carIndex++) {
            var carId = "CAR-%02d".formatted(carIndex);
            var sensorStates = sensorCatalog.statesFor(carId, loadDial.sensorsPerCar());
            var emitter = new CarEmitter(sensorStates, dispatcher, loadDial);
            emitters.add(emitter);
            emitterThreads.add(Thread.ofVirtual().name("emitter-" + carId).start(emitter));
        }
        running = true;
        log.info("telemetry generator started cars={} sensorsPerCar={} rateScale={}",
                loadDial.cars(), loadDial.sensorsPerCar(), loadDial.rateScale());
    }

    public synchronized void restart() {
        stop();
        start();
    }

    public boolean isRunning() {
        return running;
    }

    public long replayBuffered() {
        return dispatcher.replayBuffered();
    }

    public String activeSinkName() {
        return dispatcher.activeSinkName();
    }

    public java.util.List<String> availableSinkNames() {
        return dispatcher.availableSinkNames();
    }

    public void useSink(String name) {
        dispatcher.useSink(name);
    }

    private void joinQuietly(Thread thread) {
        try {
            thread.join(java.time.Duration.ofSeconds(2));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
