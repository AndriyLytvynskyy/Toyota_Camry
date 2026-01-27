package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.StreamType;
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

    @Test
    public void testWatermarkRequiresBothStreams() {
        WatermarkTracker tracker = new WatermarkTracker(5);
        Instant t = Instant.parse("2024-01-01T12:00:00Z");
        tracker.updateWatermark(StreamType.AD_CLICKS, 0, t);
        // in case we don't have events in topic 'page_views'
        assertThat(tracker.getWatermark(0)).isEqualTo(Instant.MIN);
    }

    @Test
    public void testWatermarkUsesMinOfStreams() {
        WatermarkTracker tracker = new WatermarkTracker(5);

        Instant clickTime = Instant.parse("2024-01-01T12:10:00Z");
        Instant viewTime  = Instant.parse("2024-01-01T12:05:00Z");

        tracker.updateWatermark(StreamType.AD_CLICKS, 0, clickTime);
        tracker.updateWatermark(StreamType.PAGE_VIEWS, 0, viewTime);

        Instant expected =
                viewTime.minus(Duration.ofMinutes(5));

        assertThat(tracker.getWatermark(0)).isEqualTo(expected);
    }

    /**
     * Events before watermark should be marked as late.
     */
    @Test
    public void testLateEventDetection() {
        WatermarkTracker tracker = new WatermarkTracker(5);

        Instant base = Instant.parse("2024-01-01T12:10:00Z");

        tracker.updateWatermark(StreamType.AD_CLICKS, 0, base);
        tracker.updateWatermark(StreamType.PAGE_VIEWS, 0, base);

        // 6 minutes older than base → beyond allowed lateness
        Instant lateEvent =
                base.minus(Duration.ofMinutes(6));

        assertThat(tracker.isTooLate(0, lateEvent)).isTrue();
    }

    /**
     * Watermark should never go backwards, in other words:
     * Out-of-order event should NOT move watermark backwards
     */
    @Test
    public void testWatermarkMonotonicallyIncreases() {
        WatermarkTracker tracker = new WatermarkTracker(5);

        Instant t1 = Instant.parse("2024-01-01T12:00:00Z");
        Instant t2 = Instant.parse("2024-01-01T12:10:00Z");
        Instant outOfOrder = Instant.parse("2024-01-01T11:30:00Z");

        tracker.updateWatermark(StreamType.AD_CLICKS, 0, t1);
        tracker.updateWatermark(StreamType.PAGE_VIEWS, 0, t1);

        Instant w1 = tracker.getWatermark(0);

        tracker.updateWatermark(StreamType.AD_CLICKS, 0, t2);
        tracker.updateWatermark(StreamType.PAGE_VIEWS, 0, t2);

        Instant w2 = tracker.getWatermark(0);

        // here if we have out of order event - watermark should not move backward
        tracker.updateWatermark(StreamType.AD_CLICKS, 0, outOfOrder);

        Instant w3 = tracker.getWatermark(0);

        assertThat(w2).isAfter(w1);
        assertThat(w3).isEqualTo(w2);
    }

}
