package com.ebay.challenge.streamprocessor.state;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;


public class EmittedPageViewStoreTest {

    @Test
    public void lateClickDoesNotUpdateFinalizedPageView() {
        EmittedPageViewStore store = new EmittedPageViewStore();

        PageViewEvent pv = PageViewEvent.builder()
                .eventId("pv_1")
                .userId("user_1")
                .eventTime(Instant.parse("2024-01-01T12:10:00Z"))
                .url("https://example.com")
                .build();

        store.recordEmittedPageView(pv, null);

        // And: watermark has advanced beyond the page view time
        Instant watermark = Instant.parse("2024-01-01T12:15:00Z");

        // When: a late click arrives that would otherwise be valid
        AdClickEvent lateClick = AdClickEvent.builder()
                .clickId("click_late")
                .userId("user_1")
                .campaignId("campaign_X")
                .eventTime(Instant.parse("2024-01-01T12:05:00Z"))
                .build();

        AtomicInteger updatesEmitted = new AtomicInteger(0);

        store.tryUpdateWithClick(
                lateClick,
                watermark,
                apv -> updatesEmitted.incrementAndGet()
        );

        // Then: no update should be emitted
        assertThat(updatesEmitted.get()).isEqualTo(0);

        // And: page view should still exist until eviction is called
        assertThat(store.size()).isEqualTo(1);

        // When: eviction runs
        store.evictFinalizedPageViews(watermark);

        // Then: page view is removed
        assertThat(store.size()).isEqualTo(0);
    }


}
