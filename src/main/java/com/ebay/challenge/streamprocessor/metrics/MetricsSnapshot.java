package com.ebay.challenge.streamprocessor.metrics;

import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of metrics exposed to the dashboard.
 *
 * This is a READ MODEL:
 * - No logic
 * - No defaults like Instant.MIN
 * - Nulls indicate "not yet initialized"
 */
public record MetricsSnapshot(

        /* -------- Counters -------- */
        long clicksReceived,
        long pageViewsReceived,
        long pageViewsEmitted,
        long pageViewsUpdated,

        /* -------- State sizes -------- */
        long clickStateSize,
        long pageViewStateSize,

        /* -------- Join watermarks (per partition) -------- */
        List<JoinPartitionWatermark> joinWatermarks,

        /* -------- Health -------- */
        Instant lastUpdatedAt
) {}
