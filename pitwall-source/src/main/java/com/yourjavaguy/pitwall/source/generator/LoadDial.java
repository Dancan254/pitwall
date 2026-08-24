package com.yourjavaguy.pitwall.source.generator;

import com.yourjavaguy.pitwall.source.config.SourceProperties;
import com.yourjavaguy.pitwall.source.fault.FaultInjection;
import com.yourjavaguy.pitwall.source.sensor.SensorCatalog;
import com.yourjavaguy.pitwall.source.sensor.SensorDefinition;
import org.springframework.stereotype.Component;

@Component
public class LoadDial {

    private final FaultInjection faults;
    private final SensorCatalog sensorCatalog;
    private final long lapDurationNanos;

    private volatile int cars;
    private volatile int sensorsPerCar;
    private volatile double rateScale;

    public LoadDial(SourceProperties properties, FaultInjection faults, SensorCatalog sensorCatalog) {
        this.faults = faults;
        this.sensorCatalog = sensorCatalog;
        this.lapDurationNanos = properties.lapDurationSeconds() * 1_000_000_000L;
        this.cars = properties.cars();
        this.sensorsPerCar = properties.sensorsPerCar();
        this.rateScale = properties.rateScale();
    }

    public int cars() {
        return cars;
    }

    public int sensorsPerCar() {
        return sensorsPerCar;
    }

    public double rateScale() {
        return rateScale;
    }

    public long lapDurationNanos() {
        return lapDurationNanos;
    }

    public double effectiveRateMultiplier() {
        return rateScale * faults.burstMultiplier();
    }

    public long targetEventsPerSecond() {
        var samplesPerCarPerSecond = sensorCatalog.definitions(sensorsPerCar).stream()
                .mapToInt(SensorDefinition::rateHz)
                .sum();
        return Math.round(cars * samplesPerCarPerSecond * rateScale);
    }

    public void apply(int cars, int sensorsPerCar, double rateScale) {
        this.cars = cars;
        this.sensorsPerCar = sensorsPerCar;
        this.rateScale = rateScale;
    }
}
