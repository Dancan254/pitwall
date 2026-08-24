package com.yourjavaguy.pitwall.source.control;

public record SourceStatus(
        boolean running,
        String sink,
        java.util.List<String> availableSinks,
        int cars,
        int sensorsPerCar,
        double rateScale,
        long targetEventsPerSecond,
        long actualEventsPerSecond,
        long publishedEvents,
        long failedEvents,
        boolean dropoutActive,
        long bufferedEvents,
        double burstMultiplier,
        String anomalyCarId,
        String anomalySensorId
) {
}
