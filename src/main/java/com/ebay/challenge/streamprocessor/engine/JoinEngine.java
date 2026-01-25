package com.ebay.challenge.streamprocessor.engine;

import com.ebay.challenge.streamprocessor.model.AdClickEvent;
import com.ebay.challenge.streamprocessor.model.AttributedPageView;
import com.ebay.challenge.streamprocessor.model.PageViewEvent;
import com.ebay.challenge.streamprocessor.output.OutputSink;
import com.ebay.challenge.streamprocessor.state.ClickStateStore;
import com.ebay.challenge.streamprocessor.state.EmittedPageViewStore;
import com.ebay.challenge.streamprocessor.state.WatermarkTracker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core join engine that performs windowed attribution joins between page views and ad clicks.
 *
 * Join semantics:
 * - For each page_view, find the most recent ad_click for the same user
 *   within 30 minutes before the page view (in event time)
 * - Handle out-of-order arrivals through watermark tracking
 *
 * TODO: Implement the windowed join logic
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JoinEngine {

    private final ClickStateStore clickStore;
    private final WatermarkTracker watermarkTracker;
    private final OutputSink outputSink;
    private final EmittedPageViewStore emittedPageViewStore;

    /**
     * Process an ad click event.
     * Store the click in state for future attribution.
     *
     * - Check if event is too late using watermarkTracker
     * - Store the click in clickStore
     * - Update watermark for the partition
     *
     * @param click the ad click event
     */
    public void processClick(AdClickEvent click) {
        log.debug("Processing click: {}", click.getClickId());
        int partition = click.getPartition();
        Instant eventTime = click.getEventTime();

        if (watermarkTracker.isTooLate(partition, eventTime)){
            log.warn("We drop late ad click {} (partition = {}, eventTime = {}) ",
                    click.getClickId(), click.getPartition(), click.getEventTime());
            // we just return here and do nothing
            return;
        }
        clickStore.addClick(click);

        watermarkTracker.updateWatermark(partition, eventTime);

        emittedPageViewStore.tryUpdateWithClick(click, outputSink::write);

        log.debug("Stored ad click {} for user {} at {}",
                    click.getClickId(), click.getUserId(), click.getEventTime()
                );

    }

    /**
     * Process a page view event.
     * Find matching click and emit attributed page view.
     *
     * - Check if event is too late using watermarkTracker
     * - Find attributable click from clickStore
     * - Create and emit AttributedPageView
     * - Update watermark for the partition
     *
     * @param pageView the page view event
     */
    public void processPageView(PageViewEvent pageView) {
        log.debug("Processing page view: {}", pageView.getEventId());
        int partition = pageView.getPartition();
        Instant eventTime = pageView.getEventTime();

        if (watermarkTracker.isTooLate(partition, eventTime)){
            log.warn("We drop late page view event {} (partition = {}, eventTime = {}) ",
                    pageView.getEventId(), pageView.getPartition(), pageView.getEventTime());
            // page view is late, just return
            return;
        }

        AdClickEvent click = clickStore.findAttributableClick(pageView.getUserId(), eventTime);

        AttributedPageView attributed = buildAttributedPageView(pageView, click);
        outputSink.write(attributed);

        watermarkTracker.updateWatermark(partition, eventTime);
        emittedPageViewStore.recordEmittedPageView(pageView, click);
        log.info("Emitted attributed page view {} (user = {}, click = {})", pageView.getEventId(),
                pageView.getUserId(), click != null ? click.getClickId(): "none");
    }

    private AttributedPageView buildAttributedPageView(PageViewEvent pageView, AdClickEvent click){
        return AttributedPageView.builder()
                .pageViewId(pageView.getEventId())
                .userId(pageView.getUserId())
                .eventTime(pageView.getEventTime())
                .url(pageView.getUrl())
                .attributedCampaignId(
                        click != null ? click.getCampaignId() : null
                )
                .attributedClickId(
                        click != null ? click.getClickId() : null
                )
                .build();

    }


    /**
     * Scheduled task to evict old clicks from state.
     * Runs every 30 seconds to prevent unbounded memory growth.
     *
     * - Evict clicks older than the watermark cutoff
     * - Use clickStore.evictOldClicks() with appropriate cutoff time
     */
//    @Scheduled(fixedRate = 30000)
    public void evictOldClicks() {
        log.debug("Running state eviction");
        Instant cutoffTime =
                Instant.now()
                        .minus(watermarkTracker.getAllowedLateness())
                        .minus(ClickStateStore.ATTRIBUTION_WINDOW);


        int numberOfEventsEvicted = clickStore.evictOldClicks(cutoffTime);
        if (numberOfEventsEvicted > 0) {
            log.debug("Number of evicted click events {}", numberOfEventsEvicted);
        }


    }

    /**
     * Scheduled task to evict finalized page views
     * Runs every 30 seconds to prevent unbounded memory growth.
     *
     * - Evict pageViews older than the watermark cutoff
     * - Use emittedPageViewStore.evictFinalized() with appropriate cutoff time
     */
//    @Scheduled(fixedRate = 30000)
    public void evictFinalizedPageViews() {
        Instant cutoffTime =
                Instant.now()
                        .minus(watermarkTracker.getAllowedLateness())
                        .minus(ClickStateStore.ATTRIBUTION_WINDOW);

        int numberOfEmittedPageViewsEvicted =  emittedPageViewStore.evictFinalized(cutoffTime);
        if (numberOfEmittedPageViewsEvicted > 0) {
            log.debug("Number of evicted page view events {}", numberOfEmittedPageViewsEvicted);
        }
    }
}
