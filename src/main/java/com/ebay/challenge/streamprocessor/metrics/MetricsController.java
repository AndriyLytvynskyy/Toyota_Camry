package com.ebay.challenge.streamprocessor.metrics;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * REST endpoint exposing processor metrics.
 *
 * Used by dashboards, debugging tools, and tests.
 */
@RestController
@RequiredArgsConstructor
public class MetricsController {

    private final MetricsRegistry metricsRegistry;

    @GetMapping("/metrics")
    public MetricsSnapshot metrics() {
        return metricsRegistry.snapshot();
    }
}
