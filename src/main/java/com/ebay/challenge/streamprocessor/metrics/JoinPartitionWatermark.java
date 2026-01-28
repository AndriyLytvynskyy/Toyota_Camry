package com.ebay.challenge.streamprocessor.metrics;

import java.time.Instant;

public record JoinPartitionWatermark(
    int partition,
    Instant pageViewsMaxEventTime,
    Instant adClicksMaxEventTime,
    Instant joinWatermark
) {}