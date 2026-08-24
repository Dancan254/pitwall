package com.yourjavaguy.pitwall.serving.live;

import com.yourjavaguy.pitwall.serving.config.ServingProperties;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class LiveFeed {

    private static final Logger log = LoggerFactory.getLogger(LiveFeed.class);

    private final Set<SseEmitter> subscribers = ConcurrentHashMap.newKeySet();
    private final AtomicLong deliveredEvents = new AtomicLong();
    private final AtomicLong droppedSubscribers = new AtomicLong();
    private final long timeoutMillis;

    public LiveFeed(ServingProperties properties, MeterRegistry meterRegistry) {
        this.timeoutMillis = properties.streamTimeout().toMillis();
        Gauge.builder("pitwall.serving.subscribers", subscribers, Set::size)
                .description("Browsers currently attached to the live feed")
                .register(meterRegistry);
        Gauge.builder("pitwall.serving.events.delivered", deliveredEvents, AtomicLong::doubleValue)
                .description("Server-sent events pushed to subscribers")
                .register(meterRegistry);
        Gauge.builder("pitwall.serving.subscribers.dropped", droppedSubscribers, AtomicLong::doubleValue)
                .description("Subscribers removed after a failed push")
                .register(meterRegistry);
    }

    public SseEmitter subscribe() {
        var emitter = new SseEmitter(timeoutMillis);
        subscribers.add(emitter);
        emitter.onCompletion(() -> subscribers.remove(emitter));
        emitter.onTimeout(() -> subscribers.remove(emitter));
        emitter.onError(error -> subscribers.remove(emitter));
        return emitter;
    }

    public void broadcast(String eventName, Object payload) {
        for (var subscriber : subscribers) {
            try {
                subscriber.send(SseEmitter.event().name(eventName).data(payload));
                deliveredEvents.incrementAndGet();
            } catch (IOException | IllegalStateException exception) {
                subscribers.remove(subscriber);
                droppedSubscribers.incrementAndGet();
            }
        }
    }

    public void broadcastAll(String eventName, List<?> payloads) {
        payloads.forEach(payload -> broadcast(eventName, payload));
    }

    public int subscriberCount() {
        return subscribers.size();
    }
}
