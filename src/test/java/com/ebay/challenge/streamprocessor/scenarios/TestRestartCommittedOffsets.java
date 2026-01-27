package com.ebay.challenge.streamprocessor.scenarios;

import com.ebay.challenge.streamprocessor.engine.JoinEngine;
import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.output.InMemoryOutputSink;
import com.ebay.challenge.streamprocessor.testutil.TestFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

public class TestRestartCommittedOffsets {

    @Test
    public void testRestartFromCommittedOffsets(){
        InMemoryOutputSink sink1 = new InMemoryOutputSink();
        JoinEngine engine1 = TestFactory.createJoinEngine(sink1);

        long clickOffset = 0;
        long pageViewOffset = 0;

        AdClickEvent click = AdClickEvent.builder()
                .clickId("click_1")
                .userId("user_1")
                .campaignId("campaign_A")
                .eventTime(Instant.parse("2024-01-01T12:00:00Z"))
                .partition(0)
                .offset(clickOffset++)
                .build();

        PageViewEvent pv = PageViewEvent.builder()
                .eventId("pv_1")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:05:00Z"))
                .url("https://example.com")
                .partition(0)
                .offset(pageViewOffset++)
                .build();

        engine1.processClick(click);
        engine1.processPageView(pv);

        // Simulate offset commit AFTER processing
        long committedPvOffset = pv.getOffset();

        // Assert output before crash
        assertThat(sink1.records()).hasSize(1);
        assertThat(sink1.records().getFirst().getAttributedClickId())
                .isEqualTo("click_1");

        // ---------- Here we stopped and we are going to restart----------
        engine1 = null;

        // ---------- Phase 2: Restart ----------
        InMemoryOutputSink sink2 = new InMemoryOutputSink();
        JoinEngine engine2 = TestFactory.createJoinEngine(sink2);

        // Resume from committed offsets + 1
        // (no replay of previous events)
        PageViewEvent pv2 = PageViewEvent.builder()
                .eventId("pv_2")
                .userId("user_2")
                .eventTime(Instant.parse("2024-01-01T12:10:00Z"))
                .url("https://example.com/2")
                .partition(0)
                .offset(committedPvOffset + 1)
                .build();

        engine2.processPageView(pv2);

        // ---------- Assertions ----------
        // No duplicate emission for pv_1
        assertThat(sink2.records())
                .extracting(AttributedPageView::getPageViewId)
                .containsExactly("pv_2");
    }
}
