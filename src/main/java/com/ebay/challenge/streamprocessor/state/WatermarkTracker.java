package com.ebay.challenge.streamprocessor.state;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks watermarks per partition to handle out-of-order events.
 * <p>
 * Watermark represents the point in event-time up to which we believe we have seen all events.
 * Events arriving with event_time < watermark - allowedLateness are considered too late.
 * <p>
 * TODO: Implement per-partition watermark tracking
 */
@Slf4j
@Component
public class WatermarkTracker {

    private final Duration allowedLateness;

    // TODO: Add data structure to track watermarks per partition
    // Hint: Use ConcurrentHashMap<Integer, Instant> for thread-safe partition watermarks
    // this holds partition -> maxEventTime seen
    private final ConcurrentHashMap<Integer, Instant> partitionMaxEventTimeSeen = new ConcurrentHashMap<>();

    public WatermarkTracker(@Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        log.info("Initialized WatermarkTracker with allowed lateness: {} minutes", allowedLatenessMinutes);
    }

    /**
     * Chooses later time between existing and incoming
     * @param existing current time
     * @param incoming new time
     * @return time which is later
     */
    private Instant chooseLaterInstant(Instant existing, Instant incoming) {
        return incoming.isAfter(existing) ? incoming : existing;
    }

    /**
     * Update watermark for a partition based on observed event time.
     * Watermark advances monotonically (never goes backward).
     * <p>
     *
     * - Update partition watermark if event time is later than current watermark
     * - Ensure watermark never goes backward
     * - Handle concurrent updates
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     */
    public void updateWatermark(int partition, Instant eventTime) {
        log.debug("Updating watermark for partition {} with event time {}", partition, eventTime);
        partitionMaxEventTimeSeen.merge(partition, eventTime,
                this::chooseLaterInstant);
    }

    /**
     * Get current watermark for a partition.
     * <p>
     * - Return current watermark for the partition
     * - Return Instant.MIN if partition has no watermark yet
     *
     * @param partition the partition ID
     * @return the current watermark, or Instant.MIN if not yet initialized
     */
    public Instant getWatermark(int partition) {
        Instant maxEvent = partitionMaxEventTimeSeen.get(partition);
        if (maxEvent == null){
            return Instant.MIN;
        }
        return maxEvent.minus(allowedLateness);
    }

    /**
     * Check if an event is too late (beyond allowed lateness).
     * <p>
     * - Calculate cutoff time as: watermark - allowedLateness
     * - Return true if event is before cutoff time
     * - Handle case when watermark is not yet initialized
     *
     * @param partition the partition ID
     * @param eventTime the event timestamp
     * @return true if the event is too late and should be dropped
     */
    public boolean isTooLate(int partition, Instant eventTime) {
        Instant watermark = getWatermark(partition);
        if (watermark.equals(Instant.MIN)){
            return false;
        }
        return eventTime.isBefore(watermark);
    }

    /**
     * Get the allowed lateness duration.
     *
     * @return the allowed lateness duration
     */
    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
