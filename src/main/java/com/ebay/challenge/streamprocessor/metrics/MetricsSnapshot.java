package com.ebay.challenge.streamprocessor.metrics;

import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of metrics exposed to the dashboard.
 * This is a READ MODEL:
 * - No logic
 * - No defaults like Instant.MIN
 */
public record MetricsSnapshot(

        long clicksReceived,
        long pageViewsReceived,
        long pageViewsEmitted,
        long pageViewsUpdated,

        long clickStateSize,
        long pageViewStateSize,

        List<JoinPartitionWatermark> joinWatermarks,

        Instant lastUpdatedAt
) {}
