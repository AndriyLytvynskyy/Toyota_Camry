package com.ebay.challenge.streamprocessor.metrics;

/**
 * Lightweight metrics API used by JoinEngine and exposed via /metrics.
 */

public interface Metrics {

    void onClickReceived();

    void onPageViewReceived();

    void onPageViewEmitted();

    void onPageViewUpdated(int updates);

    void onClickStateSizeUpdated(long size);

    void onPageViewStateSizeUpdated(long size);

    MetricsSnapshot snapshot();
}


