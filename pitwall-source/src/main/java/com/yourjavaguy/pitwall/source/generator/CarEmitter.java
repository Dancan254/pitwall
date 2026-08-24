package com.yourjavaguy.pitwall.source.generator;

import com.yourjavaguy.pitwall.source.sensor.SensorState;
import com.yourjavaguy.pitwall.source.sink.TelemetryDispatcher;

import java.util.List;
import java.util.concurrent.locks.LockSupport;

final class CarEmitter implements Runnable {

    private final List<SensorState> sensorStates;
    private final TelemetryDispatcher dispatcher;
    private final LoadDial loadDial;

    private volatile boolean running = true;

    CarEmitter(List<SensorState> sensorStates, TelemetryDispatcher dispatcher, LoadDial loadDial) {
        this.sensorStates = sensorStates;
        this.dispatcher = dispatcher;
        this.loadDial = loadDial;
    }

    void stop() {
        running = false;
    }

    @Override
    public void run() {
        var startNanos = System.nanoTime();
        while (running && !Thread.currentThread().isInterrupted()) {
            var now = System.nanoTime();
            var rateMultiplier = loadDial.effectiveRateMultiplier();
            var lapPhase = lapPhase(now - startNanos);
            var earliestDueNanos = Long.MAX_VALUE;

            for (var sensorState : sensorStates) {
                if (sensorState.isDue(now)) {
                    dispatcher.dispatch(sensorState.nextEvent(lapPhase));
                    sensorState.scheduleNext(now, rateMultiplier);
                }
                earliestDueNanos = Math.min(earliestDueNanos, sensorState.nextDueNanos());
            }

            var sleepNanos = earliestDueNanos - System.nanoTime();
            if (sleepNanos > 0) {
                LockSupport.parkNanos(sleepNanos);
            }
        }
    }

    private double lapPhase(long elapsedNanos) {
        return (double) (elapsedNanos % loadDial.lapDurationNanos()) / loadDial.lapDurationNanos();
    }
}
