package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.output.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.EmittedPageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

public class JoinEngineTest {

    private JoinEngine newEngine(InMemoryOutputSink sink, int latenessMinutes) {
        return new JoinEngine(
                new ClickStateStore(),
                new WatermarkTracker(latenessMinutes),
                sink,
                new EmittedPageViewStore()
        );
    }

    private PageViewEvent pageView(String id, String user, Instant t, int partition) {
        PageViewEvent pv = new PageViewEvent();
        pv.setEventId(id);
        pv.setUserId(user);
        pv.setEventTime(t);
        pv.setUrl("/page");
        pv.setPartition(partition);
        return pv;
    }

    private AdClickEvent click(
            String clickId,
            String userId,
            Instant eventTime,
            int partition
    ) {
        AdClickEvent click = new AdClickEvent();
        click.setClickId(clickId);
        click.setUserId(userId);
        click.setEventTime(eventTime);
        click.setPartition(partition);
        return click;
    }

    /**
     * Click happens before page view, then it will still 'attribute'.
     */
    @Test
    void testClickBeforePageView() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = newEngine(sink,5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        engine.processClick(
                click("click1", "user1", base.plusSeconds(300), 0) // 5 mins after base time
        );

        engine.processPageView(
                pageView("pv1", "user1", base.plusSeconds(600), 0) // 10 mins after base timr
        );

        assertThat(sink.getAll()).hasSize(1);
        assertThat(sink.getAll().get(0).getAttributedClickId())
                .isEqualTo("click1");
    }

    /**
     * Test multiple clicks in window.
     *
     * Scenario:
     * - Click1 - after 5mins
     * - Click2 - after 10 mins
     * - Page view = after 15 mins
     * - Should attribute to Click2 (latest)
     */
    @Test
    void testMultipleClicksPicksLatest() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = newEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        engine.processClick(
                click("click1", "user1", base.plusSeconds(300), 0) // after 5 mins
        );

        engine.processClick(
                click("click2", "user1", base.plusSeconds(600), 0) // after 10 mins
        );

        engine.processPageView(
                pageView("pv1", "user1", base.plusSeconds(900), 0) // after 15 mins
        );

        assertThat(sink.getAll()).hasSize(1);
        assertThat(sink.getAll().get(0).getAttributedClickId())
                .isEqualTo("click2");
    }

    /**
     * Test click outside 30-minute window.
     *
     * Scenario:
     * - Click at T+0
     * - Page view at T+35
     * - Should NOT attribute (>30 min gap)
     */
    @Test
    void testClickOutsideWindowNotAttributed() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = newEngine(sink, 5);

        Instant base = Instant.parse("2024-01-01T12:00:00Z");

        // Click at T+0
        engine.processClick(
                click("c1", "user1", base, 0)
        );

        // Page view at T+35 (> 30 minutes later)
        engine.processPageView(
                pageView("pv1", "user1", base.plusSeconds(2100), 0)
        );

        assertThat(sink.getAll()).hasSize(1);
        assertThat(sink.getAll().get(0).getAttributedClickId())
                .isNull();
    }

    /**
     * Click arrives after page view but within allowed lateness.
     *
     * Scenario:
     * - Page view at T+10 arrives first
     * - Click at T+5 arrives later (event-time earlier)
     * - Should still attribute
     */
    @Test
    void testOutOfOrderClickArrival() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = newEngine(sink, 5); // 5 min lateness

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        // Page view arrives first
        engine.processPageView(
                pageView("pv1", "user1", base.plusSeconds(600), 0) // +10 mins
        );

        // Click arrives later but is within lateness window
        engine.processClick(
                click("click1", "user1", base.plusSeconds(300), 0) // +5 mins
        );

        // in memory sink should have size of 2
        assertThat(sink.getAll()).hasSize(2);
        //second element of the list should be attributed with 'click1'
        assertThat(sink.getAll().get(1).getAttributedClickId())
                .isEqualTo("click1");
    }

    /**
     * Event arriving beyond allowed lateness should be dropped.
     *
     *  Scenario:
     * - Watermark advances past T+7
     * - Click at T+5 arrives
     * - Should be dropped
     */
    @Test
    void testVeryLateEventDropped() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = newEngine(sink, 1); // 1 min lateness

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        // Advance watermark with a newer event
        engine.processClick(
                click("new", "user1", base.plusSeconds(600), 0) // +10 mins
        );

        // Very late click
        engine.processClick(
                click("late", "user1", base.plusSeconds(300), 0) // +5 mins
        );

        engine.processPageView(
                pageView("pv1", "user1", base.plusSeconds(700), 0) //+11 mins
        );

        assertThat(sink.getAll()).hasSize(1);
        assertThat(sink.getAll().get(0).getAttributedClickId()).isEqualTo("new");
    }

    /**
     * Test page view with no matching click.
     *
     * Scenario:
     * - Page view for user with no clicks
     * - Should emit with null attribution
     */
    @Test
    void testPageViewWithNoClick() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = new JoinEngine(
                new ClickStateStore(),
                new WatermarkTracker(5),
                sink,
                new EmittedPageViewStore()
        );

        Instant pageViewTime = Instant.parse("2026-01-24T12:00:00Z");

        // Page view arrives for a user with no clicks
        engine.processPageView(
                pageView("pv1", "user-with-no-clicks", pageViewTime, 0)
        );

        // One output should be emitted
        assertThat(sink.getAll()).hasSize(1);

        // Attribution should be null
        AttributedPageView output = sink.getAll().get(0);
        assertThat(output.getAttributedClickId()).isNull();
        assertThat(output.getAttributedCampaignId()).isNull();
    }



}
