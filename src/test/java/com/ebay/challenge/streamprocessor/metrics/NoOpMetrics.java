package com.ebay.challenge.streamprocessor.metrics;

import java.time.Instant;
import java.util.ArrayList;

/**
 * No-op metrics implementation.
 * Used for tests or when metrics collection is disabled.
 */
public class NoOpMetrics implements Metrics {

    /* -------- Input -------- */

    @Override
    public void onClickReceived() {
        // no-op
    }

    @Override
    public void onPageViewReceived() {
        // no-op
    }

    /* -------- Output -------- */

    @Override
    public void onPageViewEmitted() {
        // no-op
    }

    @Override
    public void onPageViewUpdated(int updates) {
        // no-op
    }


    /* -------- State -------- */

    @Override
    public void onClickStateSizeUpdated(long size) {
        // no-op
    }

    @Override
    public void onPageViewStateSizeUpdated(long size) {
        // no-op
    }

    /* -------- Snapshot -------- */

    @Override
    public MetricsSnapshot snapshot() {
        return new MetricsSnapshot(
                0,          // clicksReceived
                0,          // pageViewsReceived
                0,          // pageViewsEmitted
                0,          // pageViewsUpdated
                0,          // clickStateSize
                0,          // pageViewStateSize
                new ArrayList<>(),
                Instant.now()
        );
    }
}
