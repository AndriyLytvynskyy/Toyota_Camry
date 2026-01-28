package com.ebay.challenge.streamprocessor.scenarios;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.output.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.testutil.TestFactory;
import org.junit.jupiter.api.Test;

import static com.ebay.challenge.streamprocessor.testutil.TestFactory.click;
import static com.ebay.challenge.streamprocessor.testutil.TestFactory.pageView;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

public class TestLateData {

    @Test
    void testLateClickIsDropped(){
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 1); // 1 minute lateness
        Instant base = Instant.parse("2026-01-27T12:00:00Z");
        engine.processClick(
                click("new_click", "user1", base.plusSeconds(600), 0) // T + 10
        );

        // Very late click (event-time behind watermark)
        engine.processClick(
                click("late_click", "user1", base.plusSeconds(300), 0) // T + 5
        );

        // page view after watermark
        engine.processPageView(
                pageView("pv1", "user1", base.plusSeconds(700), 0) // T + 11
        );

        // Only one output (page view)
        assertThat(sink.records()).hasSize(1);

        // Attribution should be from the newer click
        AttributedPageView result = sink.records().getFirst();
        assertThat(result.getAttributedClickId()).isEqualTo("new_click");

    }

    @Test
    void testLatePageViewIsDropped() {
        InMemoryOutputSink sink = new InMemoryOutputSink();
        JoinEngine engine = TestFactory.createJoinEngine(sink, 1); // 1 minute lateness

        Instant base = Instant.parse("2026-01-27T12:00:00Z");

        // Step 1: Advance clicks stream
        engine.processClick(
                click("click_new", "user1", base.plusSeconds(600), 0) // T + 10
        );

        // Step 2: Advance page view stream
        engine.processPageView(
                pageView("pv_on_time", "user1", base.plusSeconds(660), 0) // T + 11
        );

         /*
           watermark = min(12:10, 12:11) - 1min = 12:09
         */

        // Step 3: Very late page view
        engine.processPageView(
                pageView("pv_late", "user1", base.plusSeconds(300), 0) // T + 5
        );

        assertThat(sink.records()).hasSize(1);

        AttributedPageView result = sink.records().getFirst();
        assertThat(result.getPageViewId()).isEqualTo("pv_on_time");
    }

}
