package com.yourjavaguy.pitwall.serving.web;

import com.yourjavaguy.pitwall.commons.event.TelemetryAlert;
import com.yourjavaguy.pitwall.serving.live.AlertHistory;
import com.yourjavaguy.pitwall.serving.live.LiveFeed;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.List;

@RestController
@RequestMapping("/api/v1/live")
public class LiveFeedController {

    private final LiveFeed liveFeed;
    private final AlertHistory alertHistory;

    public LiveFeedController(LiveFeed liveFeed, AlertHistory alertHistory) {
        this.liveFeed = liveFeed;
        this.alertHistory = alertHistory;
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return liveFeed.subscribe();
    }

    @GetMapping("/alerts")
    public ResponseEntity<List<TelemetryAlert>> recentAlerts() {
        return ResponseEntity.ok(alertHistory.mostRecent());
    }
}
