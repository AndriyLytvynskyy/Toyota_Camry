package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.StreamType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks event-time watermarks for Option B (emit-on-watermark).
 *
 * A watermark represents the event-time boundary before which the system
 * assumes no more events will arrive (accounting for allowed lateness).
 *
 * Logical partition id is:
 *   "<topic>_<partition>"
 *
 * Example:
 *   ad_clicks_0
 *   page_views_0
 *
 * For join correctness, the effective watermark for a Kafka partition is:
 *
 *   min(
 *     maxEventTime(ad_clicks_<partition>),
 *     maxEventTime(page_views_<partition>)
 *   ) - allowedLateness
 */
@Slf4j
@Component
public class WatermarkTracker {

    private final Duration allowedLateness;

    /**
     * logicalPartitionId -> maxEventTimeSeen
     * Example key: "ad_clicks_0" - we need to know which stream it is, not to mix partitions together
     */
    private final Map<String, Instant> partitionMaxEventTimeSeen =
            new ConcurrentHashMap<>();

    public WatermarkTracker(
            @Value("${watermark.allowed-lateness-minutes:2}") int allowedLatenessMinutes
    ) {
        this.allowedLateness = Duration.ofMinutes(allowedLatenessMinutes);
        log.info(
                "Initialized WatermarkTracker with allowed lateness: {} minutes",
                allowedLatenessMinutes
        );
    }

    /**
     * Update watermark for a partition based on observed event time.
     * Watermark advances monotonically (never goes backward).
     *
     * Update partition watermark if event time is later than current watermark
     * Ensure watermark never goes backward
     * Handle concurrent updates
     *
     * @param stream e.g. "ad_clicks" or page_views
     * @param partition e.g. "partition number"
     * @param eventTime event-time timestamp
     */
    public void updateWatermark(StreamType stream, int partition, Instant eventTime) {
        String logicalPartitionId = stream.logicalPartition(partition);
        log.debug("Updating watermark for partition {} with event time {}", logicalPartitionId, eventTime);
        partitionMaxEventTimeSeen.merge(
                logicalPartitionId,
                eventTime,
                this::chooseLaterInstant
        );
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
        Instant clickMax =
                partitionMaxEventTimeSeen.get(
                        StreamType.AD_CLICKS.logicalPartition(partition)
                );

        Instant viewMax =
                partitionMaxEventTimeSeen.get(
                        StreamType.PAGE_VIEWS.logicalPartition(partition)
                );

        if (clickMax == null || viewMax == null) {
            return Instant.MIN;
        }

        Instant minMax =
                clickMax.isBefore(viewMax) ? clickMax : viewMax;

        return minMax.minus(allowedLateness);
    }

    /**
     * Check if an event is too late to be processed.
     *
     * An event is late if:
     *   event_time < effective watermark
     */
    public boolean isTooLate(int partition, Instant eventTime) {
        Instant watermark = getWatermark(partition);
        return !watermark.equals(Instant.MIN)
                && eventTime.isBefore(watermark);
    }

    private Instant chooseLaterInstant(Instant existing, Instant incoming) {
        return incoming.isAfter(existing) ? incoming : existing;
    }

    public Duration getAllowedLateness() {
        return allowedLateness;
    }
}
