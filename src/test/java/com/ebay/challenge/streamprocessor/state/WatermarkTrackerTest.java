package com.ebay.challenge.streamprocessor.state;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;


public class WatermarkTrackerTest {

    /**
     * Watermark should be None before any events.
     */
    @Test
    public void testInitialWatermarkIsMin() {
        WatermarkTracker tracker = new WatermarkTracker(5);
        assertThat(tracker.getWatermark(0)).isEqualTo(Instant.MIN);
    }

    /**
     * Watermark should update as events arrive.
     */
    @Test
    public void testWatermarkUpdatesWithEvents() {
        WatermarkTracker tracker = new WatermarkTracker(5);
        Instant eventTime = Instant.parse("2024-01-01T12:00:00Z");
        tracker.updateWatermark(0, eventTime);
        Instant expectedWatermark =
                eventTime.minus(Duration.ofMinutes(5));
        assertThat(tracker.getWatermark(0))
                .isEqualTo(expectedWatermark);
    }

    /**
     * Events before watermark should be marked as late.
     */
    @Test
    public void testLateEventDetection() {
        WatermarkTracker tracker = new WatermarkTracker(5);
        Instant baseTime = Instant.parse("2026-01-01T12:00:00Z");
        tracker.updateWatermark(0, baseTime);
        // Event older than watermark - lateness
        Instant lateEvent =
                baseTime.minus(Duration.ofMinutes(6));

        boolean isLate =
                tracker.isTooLate(0, lateEvent);

        assertThat(isLate).isTrue();
    }

    /**
     * Watermark should never go backwards, in other words:
     * Out-of-order event should NOT move watermark backwards
     */
    @Test
    public void testWatermarkMonotonicallyIncreases() {
        WatermarkTracker tracker = new WatermarkTracker(5);
        Instant eventOne = Instant.parse("2026-01-24T12:00:00Z");
        Instant eventTwo = Instant.parse("2026-01-24T12:10:00Z");
        Instant eventOutOfOrder = Instant.parse("2026-01-20T12:05:00Z");

        tracker.updateWatermark(0, eventOne);
        Instant watermarkAfterFirst = tracker.getWatermark(0);

        tracker.updateWatermark(0, eventTwo);
        Instant watermarkAfterSecond = tracker.getWatermark(0);

        tracker.updateWatermark(0, eventOutOfOrder);
        Instant watermarkAfterOutOfOrder = tracker.getWatermark(0);

        assertThat(watermarkAfterSecond)
                .isAfter(watermarkAfterFirst);

        assertThat(watermarkAfterOutOfOrder)
                .isEqualTo(watermarkAfterSecond);
    }



}
