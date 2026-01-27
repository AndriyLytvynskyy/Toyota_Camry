package com.ebay.challenge.streamprocessor.scenarios;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.output.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.testutil.TestFactory;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static com.ebay.challenge.streamprocessor.testutil.TestFactory.click;
import static com.ebay.challenge.streamprocessor.testutil.TestFactory.pageView;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOutOfOrderEvents {

    @Test
    public void testOutOfOrderClicksLatestByEventTimeWins(){
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-27T12:00:00Z");

        // Click with later event-time arrives FIRST, this click is at 12:10
        engine.processClick(
                click("click_late", "user1", base.plusSeconds(600), 0) // T + 10
        );

        // Click with earlier event-time arrives AFTER (out of order) this clicks arrives after, but event time is 12:05
        engine.processClick(
                click("click_early", "user1", base.plusSeconds(300), 0) // T + 5
        );

        // Page view after both clicks
        engine.processPageView(
                pageView("pv1", "user1", base.plusSeconds(900), 0) // T + 15
        );

        assertThat(sink.records()).hasSize(1);

        // Must attribute to click_late (latest event-time)
        AttributedPageView result = sink.records().get(0);
        assertThat(result.getAttributedClickId()).isEqualTo("click_late");

    }

    @Test
    void testOutOfOrderPageViewsProcessedSeparately(){
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 5);

        Instant base = Instant.parse("2026-01-24T12:00:00Z");

        // Click first
        engine.processClick(
                click("click1", "user1", base.plusSeconds(300), 0) // T + 5
        );

        // Page view with later event-time arrives first
        engine.processPageView(
                pageView("pv_late", "user1", base.plusSeconds(900), 0) // T + 15
        );

        // Page view with earlier event-time arrives later (out of order)
        engine.processPageView(
                pageView("pv_early", "user1", base.plusSeconds(600), 0) // T + 10
        );

        assertThat(sink.records()).hasSize(2);

        // Both page views will be attributed to click1
        sink.records().forEach(pv ->
                assertThat(pv.getAttributedClickId()).isEqualTo("click1")
        );
    }
}
